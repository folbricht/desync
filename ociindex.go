package desync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

// OCIIndexArtifactType identifies desync index artifacts in an OCI registry.
const OCIIndexArtifactType = "application/vnd.desync.index.v1"

// Annotations carrying index metadata, so tools can read the shape of an index
// from its manifest without pulling what may be a very large blob.
const (
	ociIndexChunksAnnotation    = "vnd.desync.index.chunks"
	ociIndexBlobSizeAnnotation  = "vnd.desync.index.blob-size"
	ociIndexChunkSizeAnnotation = "vnd.desync.index.chunk-size"
)

// ociTagRegexp is the OCI tag grammar. Index names are used as tags, so they
// have to fit it. Kept here to fail early with a message naming the index,
// rather than letting the registry reject the reference later.
var ociTagRegexp = regexp.MustCompile(`^[\w][\w.-]{0,127}$`)

var _ IndexWriteStore = OCIIndexStore{}

// OCIIndexStore stores indexes in an OCI registry. Each index is its own
// artifact: a blob holding the index, referenced by a manifest tagged with the
// index name. Indexes are always stored in plain form, so they can share a
// repository with chunks without interfering. Chunk operations only act on
// manifests carrying the chunk artifact type, so prune leaves indexes alone
// even if one is named like a chunk ID.
type OCIIndexStore struct {
	repo     *remote.Repository
	location string
	opt      StoreOptions
	config   *ociConfigBlobState
}

// NewOCIIndexStore initializes an index store using an OCI registry as backend.
func NewOCIIndexStore(u *url.URL, creds auth.CredentialFunc, opt StoreOptions) (OCIIndexStore, error) {
	if err := opt.ValidateIndexOptions(); err != nil {
		return OCIIndexStore{}, err
	}
	repo, err := newOCIRepository(u, creds, opt, true)
	if err != nil {
		return OCIIndexStore{}, err
	}
	return OCIIndexStore{
		repo:     repo,
		location: u.String(),
		opt:      opt,
		config:   &ociConfigBlobState{},
	}, nil
}

func (s OCIIndexStore) String() string { return s.location }

// Close the store. NOP operation but needed to implement the store interface.
func (s OCIIndexStore) Close() error { return nil }

// ValidateOCIIndexName reports whether an index can be stored in an OCI
// registry under this name. The name is used as the manifest tag, so it has to
// fit the OCI tag grammar, which not every valid filename does. Exported so
// commands can reject a name before doing the work that precedes writing the
// index.
func ValidateOCIIndexName(name string) error {
	if !ociTagRegexp.MatchString(name) {
		return fmt.Errorf("index name %q cannot be used in an OCI registry: a tag must match %s", name, ociTagRegexp)
	}
	return nil
}

// tagFromName returns the manifest tag for an index, which is the index name
// itself.
func (s OCIIndexStore) tagFromName(name string) (string, error) {
	if err := ValidateOCIIndexName(name); err != nil {
		return "", fmt.Errorf("%s: %w", s, err)
	}
	return name, nil
}

// GetIndexReader returns a reader for an index from an OCI registry.
func (s OCIIndexStore) GetIndexReader(name string) (io.ReadCloser, error) {
	ctx := context.Background()
	tag, err := s.tagFromName(name)
	if err != nil {
		return nil, err
	}
	_, r, err := s.repo.FetchReference(ctx, tag)
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, NoSuchObject{name}
		}
		return nil, err
	}
	manifest, err := readOCIManifest(r)
	_ = r.Close()
	if err != nil {
		return nil, fmt.Errorf("invalid manifest for index %s in %s: %w", name, s, err)
	}
	// The tag alone can't be trusted: an unrelated artifact may sit under it.
	if manifest.ArtifactType != OCIIndexArtifactType {
		return nil, NoSuchObject{name}
	}
	if len(manifest.Layers) != 1 {
		return nil, fmt.Errorf("manifest for index %s in %s references %d blobs, expected exactly one", name, s, len(manifest.Layers))
	}
	blob, err := s.repo.Blobs().Fetch(ctx, manifest.Layers[0])
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, NoSuchObject{name}
		}
		return nil, err
	}
	return blob, nil
}

// GetIndex returns an Index structure from the store.
func (s OCIIndexStore) GetIndex(name string) (i Index, e error) {
	r, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer r.Close()
	return IndexFromReader(r)
}

// maxInMemoryIndex is the largest index serialized into memory for a push.
// Below it the blob is pushed from a bytes.Reader, which http.NewRequest can
// rewind, so the retry and re-authentication paths work. Above it the index is
// streamed instead, trading those for bounded memory.
var maxInMemoryIndex int64 = 32 << 20

// StoreIndex writes the index to the OCI registry. A registry blob has to be
// pushed with its digest and size known upfront, and an index of a large blob
// can run to hundreds of megabytes, so the index is serialized twice rather
// than buffered: once to size and digest it, then again into the push.
func (s OCIIndexStore) StoreIndex(name string, idx Index) error {
	ctx := context.Background()
	tag, err := s.tagFromName(name)
	if err != nil {
		return err
	}

	// First pass, storing nothing, to learn the size and digest.
	h := sha256.New()
	n, err := idx.WriteTo(h)
	if err != nil {
		return err
	}
	blobDesc := ocispec.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    digest.NewDigestFromBytes(digest.SHA256, h.Sum(nil)),
		Size:      n,
	}

	if err := s.ensureConfigBlob(ctx); err != nil {
		return err
	}

	// Second pass, into the push.
	if n <= maxInMemoryIndex {
		b := bytes.NewBuffer(make([]byte, 0, n))
		if _, err := idx.WriteTo(b); err != nil {
			return err
		}
		if err := s.repo.Blobs().Push(ctx, blobDesc, bytes.NewReader(b.Bytes())); err != nil {
			return err
		}
	} else {
		pr, pw := io.Pipe()
		go func() {
			_, err := idx.WriteTo(pw)
			pw.CloseWithError(err)
		}()
		err := s.repo.Blobs().Push(ctx, blobDesc, pr)
		// Unblocks the writer if the push stopped reading early.
		_ = pr.Close()
		if err != nil {
			return err
		}
	}

	manifest := ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: OCIIndexArtifactType,
		Config:       ociEmptyConfig,
		Layers:       []ocispec.Descriptor{blobDesc},
		Annotations: map[string]string{
			ocispec.AnnotationTitle:     name,
			ociIndexChunksAnnotation:    strconv.Itoa(len(idx.Chunks)),
			ociIndexBlobSizeAnnotation:  strconv.FormatInt(idx.Length(), 10),
			ociIndexChunkSizeAnnotation: fmt.Sprintf("%d:%d:%d", idx.Index.ChunkSizeMin, idx.Index.ChunkSizeAvg, idx.Index.ChunkSizeMax),
		},
	}
	mb, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	manifestDesc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, mb)
	return s.repo.Manifests().PushReference(ctx, manifestDesc, bytes.NewReader(mb), tag)
}

// ensureConfigBlob makes sure the shared empty config blob referenced by every
// index manifest exists in the registry. Same single-flight behavior as the
// chunk store's.
func (s OCIIndexStore) ensureConfigBlob(ctx context.Context) error {
	s.config.mu.Lock()
	defer s.config.mu.Unlock()
	if s.config.pushed {
		return nil
	}
	exists, err := s.repo.Blobs().Exists(ctx, ociEmptyConfig)
	if err != nil {
		return err
	}
	if !exists {
		if err := s.repo.Blobs().Push(ctx, ociEmptyConfig, bytes.NewReader(ocispec.DescriptorEmptyJSON.Data)); err != nil {
			return err
		}
	}
	s.config.pushed = true
	return nil
}
