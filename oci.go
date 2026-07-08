package desync

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

// OCIChunkArtifactType identifies desync chunk artifacts in an OCI registry.
const OCIChunkArtifactType = "application/vnd.desync.chunk.v1"

var _ WriteStore = OCIStore{}
var _ PruneStore = OCIStore{}

// OCIStore operates on chunks in an OCI registry. Every chunk is stored as its
// own artifact: a blob holding the chunk in storage format (compressed unless
// configured otherwise), referenced by a minimal image manifest that is tagged
// with the chunk ID in hex. The tag is the only place the chunk ID appears, so
// any chunk digest algorithm works, including the default SHA512/256 which OCI
// blob digests could not represent. The manifest also keeps the blob referenced,
// protecting it from registry garbage collection of unreferenced blobs.
type OCIStore struct {
	repo         *remote.Repository
	location     string
	opt          StoreOptions
	converters   Converters
	configPushed *atomic.Bool
}

// NewOCIStore initializes a store using an OCI registry as backend.
func NewOCIStore(u *url.URL, creds auth.CredentialFunc, opt StoreOptions) (OCIStore, error) {
	if u.Scheme != "oci+https" && u.Scheme != "oci+http" {
		return OCIStore{}, fmt.Errorf("unsupported scheme %s, expected oci+https or oci+http", u.Scheme)
	}
	repo, err := remote.NewRepository(u.Host + u.Path)
	if err != nil {
		return OCIStore{}, fmt.Errorf("failed to initialize oci registry store: %w", err)
	}

	// Build a TLS client config
	tlsConfig := &tls.Config{InsecureSkipVerify: opt.TrustInsecure}

	// Add client key/cert if provided
	if opt.ClientCert != "" && opt.ClientKey != "" {
		certificate, err := tls.LoadX509KeyPair(opt.ClientCert, opt.ClientKey)
		if err != nil {
			return OCIStore{}, fmt.Errorf("failed to load client certificate from %s", opt.ClientCert)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	// Load custom CA set if provided
	if opt.CACert != "" {
		certPool := x509.NewCertPool()
		b, err := os.ReadFile(opt.CACert)
		if err != nil {
			return OCIStore{}, err
		}
		if ok := certPool.AppendCertsFromPEM(b); !ok {
			return OCIStore{}, errors.New("no CA certificates found in ca-cert file")
		}
		tlsConfig.RootCAs = certPool
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig

	var rt http.RoundTripper = transport
	if opt.ErrorRetry > 0 {
		policy := &retry.GenericPolicy{
			Retryable: retry.DefaultPredicate,
			Backoff: func(attempt int, resp *http.Response) time.Duration {
				return time.Duration(attempt+1) * opt.ErrorRetryBaseInterval
			},
			MaxWait:  time.Duration(opt.ErrorRetry) * opt.ErrorRetryBaseInterval,
			MaxRetry: opt.ErrorRetry,
		}
		rt = &retry.Transport{Base: transport, Policy: func() retry.Policy { return policy }}
	}

	// If no timeout was given in config (set to 0), then use 1 minute. If timeout is negative, use 0 to
	// set an infinite timeout.
	timeout := opt.Timeout
	if timeout == 0 {
		timeout = time.Minute
	} else if timeout < 0 {
		timeout = 0
	}

	client := &auth.Client{
		Client:     &http.Client{Transport: rt, Timeout: timeout},
		Cache:      auth.NewCache(),
		Credential: creds,
	}
	client.SetUserAgent("desync")
	repo.Client = client
	repo.PlainHTTP = u.Scheme == "oci+http"

	s := OCIStore{
		repo:         repo,
		location:     u.String(),
		opt:          opt,
		converters:   opt.converters(),
		configPushed: &atomic.Bool{},
	}
	return s, nil
}

func (s OCIStore) String() string {
	return s.location
}

// Close the store. NOP operation but needed to implement the store interface.
func (s OCIStore) Close() error { return nil }

// GetChunk reads and returns one chunk from the store. The chunk's manifest is
// looked up by tag, then the blob it references is fetched.
func (s OCIStore) GetChunk(id ChunkID) (*Chunk, error) {
	ctx := context.Background()
	blobDesc, err := s.resolveChunkBlob(ctx, id)
	if err != nil {
		return nil, err
	}
	r, err := s.repo.Blobs().Fetch(ctx, blobDesc)
	if err != nil {
		// A manifest that references a blob the registry no longer has is
		// treated like a missing chunk so other stores can be tried.
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, ChunkMissing{id}
		}
		return nil, err
	}
	defer r.Close()
	b := make([]byte, blobDesc.Size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
}

// HasChunk returns true if the chunk is in the store.
func (s OCIStore) HasChunk(id ChunkID) (bool, error) {
	_, err := s.repo.Resolve(context.Background(), id.String())
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errdef.ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// StoreChunk adds a new chunk to the store. The chunk data is pushed as a blob,
// then referenced by a manifest tagged with the chunk ID.
func (s OCIStore) StoreChunk(chunk *Chunk) error {
	ctx := context.Background()
	id := chunk.ID()
	b, err := chunk.Data()
	if err != nil {
		return err
	}
	b, err = s.converters.toStorage(b)
	if err != nil {
		return err
	}
	if err := s.ensureConfigBlob(ctx); err != nil {
		return err
	}
	blobDesc := ocispec.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    digest.FromBytes(b),
		Size:      int64(len(b)),
	}
	if err := s.repo.Blobs().Push(ctx, blobDesc, bytes.NewReader(b)); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
		return err
	}
	config := ocispec.DescriptorEmptyJSON
	config.Data = nil
	manifest := ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: OCIChunkArtifactType,
		Config:       config,
		Layers:       []ocispec.Descriptor{blobDesc},
		Annotations:  map[string]string{ocispec.AnnotationTitle: id.String() + ".cacnk"},
	}
	mb, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(mb),
		Size:      int64(len(mb)),
	}
	return s.repo.Manifests().PushReference(ctx, manifestDesc, bytes.NewReader(mb), id.String())
}

// RemoveChunk deletes a chunk, typically an invalid one, from the store.
// Used when verifying and repairing caches. Only the manifest is deleted,
// the blob is left for the registry's garbage collection.
func (s OCIStore) RemoveChunk(id ChunkID) error {
	ctx := context.Background()
	desc, err := s.repo.Resolve(ctx, id.String())
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return ChunkMissing{id}
		}
		return err
	}
	return s.repo.Manifests().Delete(ctx, desc)
}

// Prune removes any chunks from the store that are not referenced in the
// list of chunks. Only tags that parse as chunk IDs are considered, other
// artifacts sharing the repository are left alone. Just the chunk manifests
// are deleted, reclaiming the space of the now unreferenced blobs is left
// to the registry's garbage collection.
func (s OCIStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	return s.repo.Tags(ctx, "", func(tags []string) error {
		for _, tag := range tags {
			// See if we're meant to stop
			select {
			case <-ctx.Done():
				return Interrupted{}
			default:
			}

			id, err := ChunkIDFromString(tag)
			if err != nil {
				continue
			}

			// Drop the chunk if it's not on the list
			if _, ok := ids[id]; !ok {
				if err = s.RemoveChunk(id); err != nil && !errors.Is(err, ChunkMissing{id}) {
					return err
				}
			}
		}
		return nil
	})
}

// resolveChunkBlob fetches the chunk's manifest by tag and returns the
// descriptor of the blob holding the chunk data.
func (s OCIStore) resolveChunkBlob(ctx context.Context, id ChunkID) (ocispec.Descriptor, error) {
	_, r, err := s.repo.FetchReference(ctx, id.String())
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return ocispec.Descriptor{}, ChunkMissing{id}
		}
		return ocispec.Descriptor{}, err
	}
	defer r.Close()
	mb, err := io.ReadAll(r)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(mb, &manifest); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("invalid manifest for chunk %s in %s: %w", id, s, err)
	}
	if len(manifest.Layers) != 1 {
		return ocispec.Descriptor{}, fmt.Errorf("manifest for chunk %s in %s references %d blobs, expected exactly one", id, s, len(manifest.Layers))
	}
	return manifest.Layers[0], nil
}

// ensureConfigBlob uploads the shared empty config blob that every chunk
// manifest references. It only needs to exist once per repository, so the
// check-and-push runs once per store instance.
func (s OCIStore) ensureConfigBlob(ctx context.Context) error {
	if s.configPushed.Load() {
		return nil
	}
	desc := ocispec.DescriptorEmptyJSON
	desc.Data = nil
	exists, err := s.repo.Blobs().Exists(ctx, desc)
	if err != nil {
		return err
	}
	if !exists {
		err := s.repo.Blobs().Push(ctx, desc, bytes.NewReader(ocispec.DescriptorEmptyJSON.Data))
		if err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
			return err
		}
	}
	s.configPushed.Store(true)
	return nil
}
