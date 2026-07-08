package desync

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withSHA256Digest(t *testing.T) {
	t.Helper()
	prev := Digest
	Digest = SHA256{}
	t.Cleanup(func() { Digest = prev })
}

func TestOCIStoreRequiresSHA256(t *testing.T) {
	u, err := url.Parse("oci+https://registry.example.com/user/repo")
	require.NoError(t, err)

	// The default digest algorithm (SHA512/256) is not supported by OCI registries
	_, err = NewOCIStore(u, nil, StoreOptions{})
	require.Error(t, err)

	withSHA256Digest(t)
	_, err = NewOCIStore(u, nil, StoreOptions{})
	require.NoError(t, err)
}

func TestOCIStorePlainHTTP(t *testing.T) {
	withSHA256Digest(t)

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

// testOCIRegistry implements just enough of the OCI distribution API to
// serve as a blob store for a single repository named "user/repo".
type testOCIRegistry struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

func (reg *testOCIRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	const blobPath = "/v2/user/repo/blobs/"
	const uploadPath = "/v2/user/repo/blobs/uploads/"
	switch {
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && len(r.URL.Path) > len(blobPath) && r.URL.Path[:len(blobPath)] == blobPath:
		b, ok := reg.blobs[r.URL.Path[len(blobPath):]]
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
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestOCIStoreRoundtrip(t *testing.T) {
	withSHA256Digest(t)

	srv := httptest.NewServer(&testOCIRegistry{blobs: make(map[string][]byte)})
	defer srv.Close()

	u, err := url.Parse("oci+" + srv.URL + "/user/repo")
	require.NoError(t, err)
	s, err := NewOCIStore(u, nil, StoreOptions{})
	require.NoError(t, err)

	data := []byte("some chunk data")
	chunk := NewChunk(data)

	// The chunk shouldn't be there yet
	hasChunk, err := s.HasChunk(chunk.ID())
	require.NoError(t, err)
	assert.False(t, hasChunk)
	_, err = s.GetChunk(chunk.ID())
	require.ErrorIs(t, err, ChunkMissing{chunk.ID()})

	// Store it, then read it back
	require.NoError(t, s.StoreChunk(chunk))

	hasChunk, err = s.HasChunk(chunk.ID())
	require.NoError(t, err)
	assert.True(t, hasChunk)

	got, err := s.GetChunk(chunk.ID())
	require.NoError(t, err)
	b, err := got.Data()
	require.NoError(t, err)
	assert.Equal(t, data, b)
}
