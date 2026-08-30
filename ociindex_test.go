package desync

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
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

// Covers the round trip, the manifest the index is stored under, and the
// streaming path taken by indexes too large to buffer, which has to produce
// the same blob as the buffered one.
func TestOCIIndexStoreRoundtrip(t *testing.T) {
	s, _, reg := newTestOCIIndexStore(t)
	idx := testIndex(t)

	_, err := s.GetIndex("blob1.caibx")
	require.ErrorAs(t, err, &NoSuchObject{})

	require.NoError(t, s.StoreIndex("blob1.caibx", idx))
	got, err := s.GetIndex("blob1.caibx")
	require.NoError(t, err)
	require.Equal(t, idx.Index, got.Index)
	require.Equal(t, idx.Chunks, got.Chunks)

	// The manifest carries the shape of the index, so it can be read without
	// pulling what may be a very large blob.
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

	// Same index again over the streaming path.
	orig := maxInMemoryIndex
	maxInMemoryIndex = 1
	defer func() { maxInMemoryIndex = orig }()

	require.NoError(t, s.StoreIndex("streamed.caibx", idx))
	got, err = s.GetIndex("streamed.caibx")
	require.NoError(t, err)
	require.Equal(t, idx.Chunks, got.Chunks)

	var streamed ocispec.Manifest
	require.NoError(t, json.Unmarshal(reg.manifests["streamed.caibx"].content, &streamed))
	assert.Equal(t, manifest.Layers[0].Digest, streamed.Layers[0].Digest)
	assert.Equal(t, manifest.Layers[0].Size, streamed.Layers[0].Size)
}

// Indexes and chunks can share a repository: pruning the chunk store must not
// touch indexes, and neither kind may be mistaken for the other.
func TestOCIIndexStoreSharesRepoWithChunks(t *testing.T) {
	is, cs, _ := newTestOCIIndexStore(t)
	idx := testIndex(t)
	require.NoError(t, is.StoreIndex("blob1.caibx", idx))

	chunk := NewChunk([]byte("some chunk data"))
	require.NoError(t, cs.StoreChunk(chunk))

	// A chunk is not an index, even asked for under its own tag.
	_, err := is.GetIndex(chunk.ID().String() + ".cacnk")
	require.ErrorAs(t, err, &NoSuchObject{})

	// Prune the chunk store down to nothing. The index survives: its tag
	// doesn't parse as a chunk ID, and it carries a different artifact type.
	require.NoError(t, cs.Prune(t.Context(), map[ChunkID]struct{}{}))

	got, err := is.GetIndex("blob1.caibx")
	require.NoError(t, err)
	assert.Equal(t, idx.Chunks, got.Chunks)

	has, err := cs.HasChunk(chunk.ID())
	require.NoError(t, err)
	assert.False(t, has, "chunk should have been pruned")
}

// Names that can't be an OCI tag, and options an index store can't honor, have
// to be rejected rather than passed to the registry or silently ignored.
func TestOCIIndexStoreRejects(t *testing.T) {
	for _, name := range []string{"a", "file.iso.caibx", "rootfs-v2.caidx", "a_b.c-d"} {
		assert.NoError(t, ValidateOCIIndexName(name), name)
	}
	for _, name := range []string{"", "sub/dir.caibx", ".hidden", "-lead", "with space", strings.Repeat("a", 129)} {
		assert.Error(t, ValidateOCIIndexName(name), name)
	}

	// The store surfaces that check on both paths.
	s, _, _ := newTestOCIIndexStore(t)
	require.ErrorContains(t, s.StoreIndex(".hidden.caibx", Index{}), "cannot be used in an OCI registry")
	_, err := s.GetIndex(".hidden.caibx")
	require.ErrorContains(t, err, "cannot be used in an OCI registry")

	// Indexes are stored in plain form, so encryption can't be honored.
	u, err := url.Parse("oci+https://registry.example.com/user/repo")
	require.NoError(t, err)
	_, err = NewOCIIndexStore(u, nil, StoreOptions{Encryption: true})
	require.Error(t, err)
}

// oras returns the manifest response body unbounded, so an endless or
// oversized body has to be cut off rather than read until memory runs out.
func TestOCIManifestSizeLimit(t *testing.T) {
	// A body larger than the limit is rejected rather than consumed.
	huge := strings.NewReader("{" + strings.Repeat(" ", maxOCIManifestSize+16) + "}")
	_, err := readOCIManifest(huge)
	require.ErrorContains(t, err, "exceeds")

	// Anything trailing a valid manifest is rejected too.
	_, err = readOCIManifest(strings.NewReader(`{"schemaVersion":2} trailing`))
	require.Error(t, err)

	m, err := readOCIManifest(strings.NewReader(`{"schemaVersion":2,"artifactType":"` + OCIIndexArtifactType + `"}`))
	require.NoError(t, err)
	assert.Equal(t, OCIIndexArtifactType, m.ArtifactType)
}
