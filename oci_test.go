package desync

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOCIStoreScheme(t *testing.T) {
	u, err := url.Parse("https://registry.example.com/user/repo")
	require.NoError(t, err)
	_, err = NewOCIStore(u, nil, StoreOptions{})
	require.Error(t, err)

	tests := map[string]struct {
		url       string
		plainHTTP bool
	}{
		"https": {"oci+https://registry.example.com/user/repo", false},
		"http":  {"oci+http://127.0.0.1:5000/user/repo", true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(test.url)
			require.NoError(t, err)
			s, err := NewOCIStore(u, nil, StoreOptions{})
			require.NoError(t, err)
			require.Equal(t, test.plainHTTP, s.repo.PlainHTTP)
		})
	}
}

// testOCIManifest is a manifest held by testOCIRegistry, stored under both its
// tag and its digest.
type testOCIManifest struct {
	mediaType string
	digest    string
	content   []byte
}

// testOCIRegistry implements just enough of the OCI distribution API to serve
// as a chunk store for a single repository named "user/repo".
type testOCIRegistry struct {
	mu        sync.Mutex
	blobs     map[string][]byte
	manifests map[string]testOCIManifest
}

func newTestOCIRegistry() *testOCIRegistry {
	return &testOCIRegistry{
		blobs:     make(map[string][]byte),
		manifests: make(map[string]testOCIManifest),
	}
}

func (reg *testOCIRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	const blobPath = "/v2/user/repo/blobs/"
	const uploadPath = "/v2/user/repo/blobs/uploads/"
	const manifestPath = "/v2/user/repo/manifests/"
	switch {
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && strings.HasPrefix(r.URL.Path, uploadPath):
		w.WriteHeader(http.StatusNotFound)
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && strings.HasPrefix(r.URL.Path, blobPath):
		b, ok := reg.blobs[strings.TrimPrefix(r.URL.Path, blobPath)]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodGet {
			w.Write(b)
		}
	case r.Method == http.MethodPost && r.URL.Path == uploadPath:
		w.Header().Set("Location", uploadPath+"session")
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodPut && r.URL.Path == uploadPath+"session":
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		d := r.URL.Query().Get("digest")
		if fmt.Sprintf("sha256:%x", sha256.Sum256(b)) != d {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reg.blobs[d] = b
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodGet && r.URL.Path == "/v2/user/repo/tags/list":
		tags := []string{}
		for k := range reg.manifests {
			if !strings.HasPrefix(k, "sha256:") {
				tags = append(tags, k)
			}
		}
		sort.Strings(tags)
		b, err := json.Marshal(map[string]any{"name": "user/repo", "tags": tags})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && strings.HasPrefix(r.URL.Path, manifestPath):
		m, ok := reg.manifests[strings.TrimPrefix(r.URL.Path, manifestPath)]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(m.content)))
		w.Header().Set("Content-Type", m.mediaType)
		w.Header().Set("Docker-Content-Digest", m.digest)
		if r.Method == http.MethodGet {
			w.Write(m.content)
		}
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, manifestPath):
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		m := testOCIManifest{
			mediaType: r.Header.Get("Content-Type"),
			digest:    fmt.Sprintf("sha256:%x", sha256.Sum256(b)),
			content:   b,
		}
		reg.manifests[strings.TrimPrefix(r.URL.Path, manifestPath)] = m
		reg.manifests[m.digest] = m
		w.Header().Set("Docker-Content-Digest", m.digest)
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, manifestPath):
		ref := strings.TrimPrefix(r.URL.Path, manifestPath)
		m, ok := reg.manifests[ref]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		for k, v := range reg.manifests {
			if v.digest == m.digest {
				delete(reg.manifests, k)
			}
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// newTestOCIStore starts a fake registry and returns a store pointed at it.
func newTestOCIStore(t *testing.T, opt StoreOptions) (OCIStore, *testOCIRegistry) {
	t.Helper()
	reg := newTestOCIRegistry()
	srv := httptest.NewServer(reg)
	t.Cleanup(srv.Close)

	u, err := url.Parse("oci+" + srv.URL + "/user/repo")
	require.NoError(t, err)
	s, err := NewOCIStore(u, nil, opt)
	require.NoError(t, err)
	return s, reg
}

// The roundtrip runs with the default SHA512/256 digest algorithm and default
// compression. Only the tag references the chunk ID, so the store works with
// digest algorithms that OCI blob digests can't represent.
func TestOCIStoreRoundtrip(t *testing.T) {
	s, reg := newTestOCIStore(t, StoreOptions{})

	data := []byte("some chunk data")
	chunk := NewChunk(data)
	id := chunk.ID()

	// The chunk shouldn't be there yet
	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	assert.False(t, hasChunk)
	_, err = s.GetChunk(id)
	require.ErrorIs(t, err, ChunkMissing{id})

	// Store it, then read it back
	require.NoError(t, s.StoreChunk(chunk))

	hasChunk, err = s.HasChunk(id)
	require.NoError(t, err)
	assert.True(t, hasChunk)

	got, err := s.GetChunk(id)
	require.NoError(t, err)
	b, err := got.Data()
	require.NoError(t, err)
	assert.Equal(t, data, b)

	// The chunk's manifest must be tagged with the chunk ID to be protected
	// from registry garbage collection, and the blob must hold the chunk in
	// compressed form
	reg.mu.Lock()
	defer reg.mu.Unlock()
	assert.Contains(t, reg.manifests, id.String())
	compressed := fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	assert.NotContains(t, reg.blobs, compressed, "chunk blob should be compressed")
}

func TestOCIStoreUncompressed(t *testing.T) {
	s, reg := newTestOCIStore(t, StoreOptions{Uncompressed: true})

	data := []byte("some uncompressed chunk data")
	chunk := NewChunk(data)
	require.NoError(t, s.StoreChunk(chunk))

	got, err := s.GetChunk(chunk.ID())
	require.NoError(t, err)
	b, err := got.Data()
	require.NoError(t, err)
	assert.Equal(t, data, b)

	// The blob in the registry should hold the chunk data as-is
	reg.mu.Lock()
	defer reg.mu.Unlock()
	assert.Contains(t, reg.blobs, fmt.Sprintf("sha256:%x", sha256.Sum256(data)))
}

func TestOCIStoreRemoveChunk(t *testing.T) {
	s, _ := newTestOCIStore(t, StoreOptions{})

	chunk := NewChunk([]byte("some chunk data"))
	id := chunk.ID()
	require.NoError(t, s.StoreChunk(chunk))

	require.NoError(t, s.RemoveChunk(id))
	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	assert.False(t, hasChunk)

	// Removing a chunk that isn't there reports it as missing
	require.ErrorIs(t, s.RemoveChunk(id), ChunkMissing{id})
}

func TestOCIStorePrune(t *testing.T) {
	s, reg := newTestOCIStore(t, StoreOptions{})

	chunks := make([]*Chunk, 3)
	for i := range chunks {
		chunks[i] = NewChunk(fmt.Appendf(nil, "chunk data %d", i))
		require.NoError(t, s.StoreChunk(chunks[i]))
	}

	// Add an unrelated artifact to the same repository, it should survive the prune
	foreignContent := []byte(`{"schemaVersion":2}`)
	foreign := testOCIManifest{
		mediaType: "application/vnd.oci.image.manifest.v1+json",
		digest:    fmt.Sprintf("sha256:%x", sha256.Sum256(foreignContent)),
		content:   foreignContent,
	}
	reg.mu.Lock()
	reg.manifests["latest"] = foreign
	reg.manifests[foreign.digest] = foreign
	reg.mu.Unlock()

	// Prune everything but the first chunk
	id := chunks[0].ID()
	require.NoError(t, s.Prune(context.Background(), map[ChunkID]struct{}{id: {}}))

	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	assert.True(t, hasChunk)
	for _, chunk := range chunks[1:] {
		hasChunk, err := s.HasChunk(chunk.ID())
		require.NoError(t, err)
		assert.False(t, hasChunk)
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()
	assert.Contains(t, reg.manifests, "latest")
}

func TestOCIStoreInvalidChunk(t *testing.T) {
	s, reg := newTestOCIStore(t, StoreOptions{Uncompressed: true})

	data := []byte("some chunk data")
	chunk := NewChunk(data)
	require.NoError(t, s.StoreChunk(chunk))

	// Corrupt the blob in the registry without changing its size
	reg.mu.Lock()
	key := fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	corrupted := []byte(strings.ToUpper(string(data)))
	reg.blobs[key] = corrupted
	reg.mu.Unlock()

	_, err := s.GetChunk(chunk.ID())
	var invalid ChunkInvalid
	require.ErrorAs(t, err, &invalid)
}
