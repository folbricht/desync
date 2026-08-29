package desync

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestOCIIndexStore starts a fake registry and returns an index store and a
// chunk store pointed at the same repository.
func newTestOCIIndexStore(t *testing.T) (OCIIndexStore, OCIStore, *testOCIRegistry) {
	t.Helper()
	reg := newTestOCIRegistry()
	srv := httptest.NewServer(reg)
	t.Cleanup(srv.Close)

	u, err := url.Parse("oci+" + srv.URL + "/user/repo")
	require.NoError(t, err)
	is, err := NewOCIIndexStore(u, nil, StoreOptions{})
	require.NoError(t, err)
	cs, err := NewOCIStore(u, nil, StoreOptions{})
	require.NoError(t, err)
	return is, cs, reg
}

func testIndex(t *testing.T) Index {
	t.Helper()
	f, err := os.Open("testdata/blob1.caibx")
	require.NoError(t, err)
	defer f.Close()
	idx, err := IndexFromReader(f)
	require.NoError(t, err)
	return idx
}

func TestOCIIndexStoreRoundtrip(t *testing.T) {
	s, _, _ := newTestOCIIndexStore(t)
	idx := testIndex(t)

	_, err := s.GetIndex("blob1.caibx")
	require.ErrorAs(t, err, &NoSuchObject{})

	require.NoError(t, s.StoreIndex("blob1.caibx", idx))

	got, err := s.GetIndex("blob1.caibx")
	require.NoError(t, err)
	require.Equal(t, idx.Index, got.Index)
	require.Equal(t, idx.Chunks, got.Chunks)
}

// The index manifest carries the shape of the index, so it can be read without
// pulling what may be a very large blob.
func TestOCIIndexStoreManifest(t *testing.T) {
	s, _, reg := newTestOCIIndexStore(t)
	idx := testIndex(t)
	require.NoError(t, s.StoreIndex("blob1.caibx", idx))

	m, ok := reg.manifests["blob1.caibx"]
	require.True(t, ok, "index not tagged with its name")

	var manifest ocispec.Manifest
	require.NoError(t, json.Unmarshal(m.content, &manifest))
	assert.Equal(t, OCIIndexArtifactType, manifest.ArtifactType)
	require.Len(t, manifest.Layers, 1)
	assert.Equal(t, "blob1.caibx", manifest.Annotations[ocispec.AnnotationTitle])
	assert.Equal(t, "161", manifest.Annotations[ociIndexChunksAnnotation])
	assert.Equal(t, "2097152", manifest.Annotations[ociIndexBlobSizeAnnotation])
	assert.Equal(t, "2048:8192:32768", manifest.Annotations[ociIndexChunkSizeAnnotation])
}

// Indexes and chunks can share a repository. Pruning the chunk store must not
// touch indexes, and a chunk lookup must not be satisfied by an index.
func TestOCIIndexStoreSharesRepoWithChunks(t *testing.T) {
	is, cs, _ := newTestOCIIndexStore(t)
	idx := testIndex(t)
	require.NoError(t, is.StoreIndex("blob1.caibx", idx))

	chunk := NewChunk([]byte("some chunk data"))
	require.NoError(t, cs.StoreChunk(chunk))

	// Prune the chunk store down to nothing. The index has to survive: its
	// tag doesn't parse as a chunk ID, and it carries a different artifact type.
	require.NoError(t, cs.Prune(t.Context(), map[ChunkID]struct{}{}))

	got, err := is.GetIndex("blob1.caibx")
	require.NoError(t, err)
	assert.Equal(t, idx.Chunks, got.Chunks)

	has, err := cs.HasChunk(chunk.ID())
	require.NoError(t, err)
	assert.False(t, has, "chunk should have been pruned")
}

// An index name that isn't a valid OCI tag has to fail with a clear error
// rather than being sent to the registry.
func TestOCIIndexStoreInvalidName(t *testing.T) {
	s, _, _ := newTestOCIIndexStore(t)
	for _, name := range []string{"sub/dir.caibx", ".hidden.caibx", "-leading.caibx", ""} {
		t.Run(name, func(t *testing.T) {
			err := s.StoreIndex(name, Index{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "OCI tag")

			_, err = s.GetIndex(name)
			require.Error(t, err)
		})
	}
}

// Index stores hold plaintext, so encryption options have to be rejected
// rather than silently ignored.
func TestOCIIndexStoreRejectsEncryption(t *testing.T) {
	u, err := url.Parse("oci+https://registry.example.com/user/repo")
	require.NoError(t, err)
	_, err = NewOCIIndexStore(u, nil, StoreOptions{Encryption: true})
	require.Error(t, err)
}

// A tag holding some other kind of artifact is not an index.
func TestOCIIndexStoreForeignArtifact(t *testing.T) {
	is, cs, _ := newTestOCIIndexStore(t)

	// Store a chunk, then ask for an index under that chunk's tag.
	chunk := NewChunk([]byte("some chunk data"))
	require.NoError(t, cs.StoreChunk(chunk))

	_, err := is.GetIndex(chunk.ID().String() + ".cacnk")
	require.ErrorAs(t, err, &NoSuchObject{})
}
