package desync

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

var _ WriteStore = OCIStore{}

// OCIStore operates on chunks in an Open Container Image registry.
type OCIStore struct {
	repo       *remote.Repository
	location   string
	opt        StoreOptions
	converters Converters
}

// NewOCIStore initializes a new Open Registry As Storage backend.
func NewOCIStore(u *url.URL, creds auth.CredentialFunc, opt StoreOptions) (OCIStore, error) {
	// The OCI spec does not support desync's default hash algorithm (SHA512/256), so we must
	// be using SHA256 only.
	if Digest.Algorithm() != crypto.SHA256 {
		return OCIStore{}, errors.New("OCI stores only support SHA256, use --digest=sha256")
	}

	repo, err := remote.NewRepository(u.Host + u.Path)
	if err != nil {
		return OCIStore{}, fmt.Errorf("failed to initialize oci registry store: %w", err)
	}
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: opt.TrustInsecure,
	}
	client := &auth.Client{
		Client: &http.Client{
			Transport: retry.NewTransport(baseTransport),
		},
		Credential: creds,
	}
	client.SetUserAgent("desync")
	repo.Client = client
	repo.PlainHTTP = strings.HasSuffix(u.Scheme, "-http")
	s := OCIStore{
		repo:     repo,
		location: u.String(),
		opt:      opt,
	}
	return s, nil
}

func (s OCIStore) String() string {
	return s.location
}

// Close the store. NOP operation but needed to implement the store interface.
func (s OCIStore) Close() error { return nil }

// GetChunk reads and returns one chunk from the store
func (s OCIStore) GetChunk(id ChunkID) (*Chunk, error) {
	descriptor, err := s.repo.Blobs().Resolve(context.Background(), ociReference(id))
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, ChunkMissing{id}
		}
		return nil, err
	}
	r, err := s.repo.Fetch(context.Background(), descriptor)
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, ChunkMissing{id}
		}
		return nil, err
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
}

// StoreChunk adds a new chunk to the store.
func (s OCIStore) StoreChunk(chunk *Chunk) error {
	b, err := chunk.Data()
	if err != nil {
		return err
	}
	b, err = s.converters.toStorage(b)
	if err != nil {
		return err
	}
	descriptor := ociDescriptorForChunk(chunk.ID())
	descriptor.Size = int64(len(b))
	return s.repo.Push(context.Background(), descriptor, bytes.NewReader(b))
}

// HasChunk returns true if the chunk is in the store.
func (s OCIStore) HasChunk(id ChunkID) (bool, error) {
	return s.repo.Exists(context.Background(), ociDescriptorForChunk(id))
}

// RemoveChunk deletes a chunk, typically an invalid one, from the store.
// Used when verifying and repairing caches.
func (s OCIStore) RemoveChunk(id ChunkID) error {
	err := s.repo.Delete(context.Background(), ociDescriptorForChunk(id))
	if errors.Is(err, errdef.ErrNotFound) {
		return ChunkMissing{id}
	}
	return err
}

func ociDescriptorForChunk(id ChunkID) ocispec.Descriptor {
	return ocispec.Descriptor{
		Digest:    digest.Digest(ociReference(id)),
		MediaType: "application/vnd.oci.image.layer.v1.tar+zstd",
	}
}

func ociReference(id ChunkID) string {
	return "sha256:" + id.String()
}
