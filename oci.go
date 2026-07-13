package desync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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

// maxOCIChunkBlobSize caps the blob size accepted from a chunk manifest. Far
// above any real chunk, it only stops a corrupt or malicious manifest from
// driving a huge or invalid allocation before the blob is even fetched.
const maxOCIChunkBlobSize = 1 << 30

// retryPredicate retries any transport error, matching the error-retry
// behavior of the other network stores. Response codes are left to the
// default predicate, which retries 408, 429, and 5xx.
func retryPredicate(resp *http.Response, err error) (bool, error) {
	if err != nil {
		return true, nil
	}
	return retry.DefaultPredicate(resp, nil)
}

var _ WriteStore = OCIStore{}
var _ PruneStore = OCIStore{}

// OCIStore operates on chunks in an OCI registry. Every chunk is stored as its
// own artifact: a blob holding the chunk in storage format (compressed and/or
// encrypted as configured), referenced by a minimal image manifest that is
// tagged with the chunk ID in hex followed by the storage extension, the same
// naming used for chunk files in other stores. The tag is the only place the
// chunk ID appears, so any chunk digest algorithm works, including the default
// SHA512/256 which OCI blob digests could not represent, and chunks in
// different storage formats or with different encryption keys can coexist in
// one repository. The manifest also keeps the blob referenced, protecting it
// from registry garbage collection of unreferenced blobs.
type OCIStore struct {
	repo         *remote.Repository
	location     string
	opt          StoreOptions
	converters   Converters
	configPushed *atomic.Bool

	// Chunk tag extension, derived from the converters at construction
	extension string
}

// NewOCIStore initializes a store using an OCI registry as backend.
func NewOCIStore(u *url.URL, creds auth.CredentialFunc, opt StoreOptions) (OCIStore, error) {
	if u.Scheme != "oci+https" && u.Scheme != "oci+http" {
		return OCIStore{}, fmt.Errorf("unsupported scheme %s, expected oci+https or oci+http", u.Scheme)
	}
	repo, err := remote.NewRepository(strings.TrimSuffix(u.Host+u.Path, "/"))
	if err != nil {
		return OCIStore{}, fmt.Errorf("failed to initialize oci registry store: %w", err)
	}
	// Chunk manifests never carry a subject, so the client-side referrers
	// indexing oras-go falls back to on registries without referrers support
	// can never have anything to do. Declaring the capability stops it from
	// fetching every manifest ahead of a delete just to look for a subject.
	repo.SetReferrersCapability(true)

	tlsConfig, err := opt.tlsClientConfig()
	if err != nil {
		return OCIStore{}, err
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	transport.MaxIdleConnsPerHost = opt.N

	var rt http.RoundTripper = transport
	if opt.ErrorRetry > 0 {
		policy := &retry.GenericPolicy{
			Retryable: retryPredicate,
			Backoff: func(attempt int, resp *http.Response) time.Duration {
				return time.Duration(attempt+1) * opt.ErrorRetryBaseInterval
			},
			MaxWait:  time.Duration(opt.ErrorRetry) * opt.ErrorRetryBaseInterval,
			MaxRetry: opt.ErrorRetry,
		}
		rt = &retry.Transport{Base: transport, Policy: func() retry.Policy { return policy }}
	}

	client := &auth.Client{
		Client:     &http.Client{Transport: rt, Timeout: opt.effectiveTimeout()},
		Cache:      auth.NewCache(),
		Credential: creds,
	}
	client.SetUserAgent("desync")
	repo.Client = client
	repo.PlainHTTP = u.Scheme == "oci+http"

	converters, err := opt.StorageConverters()
	if err != nil {
		return OCIStore{}, err
	}
	s := OCIStore{
		repo:         repo,
		location:     u.String(),
		opt:          opt,
		converters:   converters,
		configPushed: &atomic.Bool{},
		extension:    converters.storageExtension(),
	}
	return s, nil
}

func (s OCIStore) String() string {
	return s.location
}

// tagFromID returns the manifest tag for a chunk, the hex ID followed by
// the storage extension. Both are well within the OCI tag grammar and
// length limit.
func (s OCIStore) tagFromID(id ChunkID) string {
	return id.String() + s.extension
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
	_, err := s.repo.Resolve(context.Background(), s.tagFromID(id))
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
	b, err := chunk.Storage(s.converters)
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
	if err := s.repo.Blobs().Push(ctx, blobDesc, bytes.NewReader(b)); err != nil {
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
		Annotations:  map[string]string{ocispec.AnnotationTitle: s.tagFromID(id)},
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
	return s.repo.Manifests().PushReference(ctx, manifestDesc, bytes.NewReader(mb), s.tagFromID(id))
}

// RemoveChunk deletes a chunk, typically an invalid one, from the store.
// Used when verifying and repairing caches. Only manifests carrying the
// desync chunk artifact type are deleted; a tag that names any other kind
// of artifact reports the chunk as missing instead. The blob is left for
// the registry's garbage collection.
func (s OCIStore) RemoveChunk(id ChunkID) error {
	ctx := context.Background()
	_, desc, err := s.fetchChunkManifest(ctx, id)
	if err != nil {
		return err
	}
	return s.repo.Manifests().Delete(ctx, desc)
}

// Prune removes any chunks from the store that are not referenced in the
// list of chunks. Only tags matching the store's chunk naming, the ID
// followed by the storage extension, are considered, and of those only
// manifests carrying the desync chunk artifact type are deleted. Other
// artifacts and chunks in different storage formats sharing the repository
// are left alone, even ones under a chunk-ID-shaped tag. Just the chunk
// manifests are deleted, reclaiming the space of the now unreferenced blobs
// is left to the registry's garbage collection.
func (s OCIStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	return s.repo.Tags(ctx, "", func(tags []string) error {
		for _, tag := range tags {
			// See if we're meant to stop
			select {
			case <-ctx.Done():
				return Interrupted{}
			default:
			}

			id, ok := chunkIDFromFilename(tag, s.extension)
			if !ok {
				continue
			}

			// Drop the chunk if it's not on the list
			if _, ok := ids[id]; !ok {
				if err := s.RemoveChunk(id); err != nil && !errors.Is(err, ChunkMissing{id}) {
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
	manifest, _, err := s.fetchChunkManifest(ctx, id)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if len(manifest.Layers) != 1 {
		return ocispec.Descriptor{}, fmt.Errorf("manifest for chunk %s in %s references %d blobs, expected exactly one", id.String(), s, len(manifest.Layers))
	}
	blobDesc := manifest.Layers[0]
	if blobDesc.Size < 0 || blobDesc.Size > maxOCIChunkBlobSize {
		return ocispec.Descriptor{}, fmt.Errorf("manifest for chunk %s in %s references a blob of invalid size %d", id.String(), s, blobDesc.Size)
	}
	return blobDesc, nil
}

// fetchChunkManifest fetches the manifest tagged for the chunk and returns it
// along with its own descriptor. A tag that doesn't exist, or one that names
// a manifest without the desync chunk artifact type, resolves to ChunkMissing:
// the tag alone can't be trusted, an unrelated artifact sharing the repository
// may sit under a chunk-ID-shaped tag, especially when the storage extension
// is empty (uncompressed, unencrypted stores).
func (s OCIStore) fetchChunkManifest(ctx context.Context, id ChunkID) (ocispec.Manifest, ocispec.Descriptor, error) {
	desc, r, err := s.repo.FetchReference(ctx, s.tagFromID(id))
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return ocispec.Manifest{}, ocispec.Descriptor{}, ChunkMissing{id}
		}
		return ocispec.Manifest{}, ocispec.Descriptor{}, err
	}
	defer r.Close()
	mb, err := io.ReadAll(r)
	if err != nil {
		return ocispec.Manifest{}, ocispec.Descriptor{}, err
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(mb, &manifest); err != nil {
		return ocispec.Manifest{}, ocispec.Descriptor{}, fmt.Errorf("invalid manifest for chunk %s in %s: %w", id.String(), s, err)
	}
	if manifest.ArtifactType != OCIChunkArtifactType {
		return ocispec.Manifest{}, ocispec.Descriptor{}, ChunkMissing{id}
	}
	return manifest, desc, nil
}

// ensureConfigBlob uploads the shared empty config blob that every chunk
// manifest references. Registries accept re-pushes of existing blobs, so
// the push is simply skipped once it has succeeded for this store instance.
func (s OCIStore) ensureConfigBlob(ctx context.Context) error {
	if s.configPushed.Load() {
		return nil
	}
	desc := ocispec.DescriptorEmptyJSON
	desc.Data = nil
	if err := s.repo.Blobs().Push(ctx, desc, bytes.NewReader(ocispec.DescriptorEmptyJSON.Data)); err != nil {
		return err
	}
	s.configPushed.Store(true)
	return nil
}
