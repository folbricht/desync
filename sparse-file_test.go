package desync

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// flakyStore wraps a Store and injects a fixed number of transient failures
// for a specific chunk before delegating to the underlying store.
type flakyStore struct {
	Store
	mu        sync.Mutex
	failID    ChunkID
	failsLeft int
}

func (s *flakyStore) GetChunk(id ChunkID) (*Chunk, error) {
	s.mu.Lock()
	if id == s.failID && s.failsLeft > 0 {
		s.failsLeft--
		s.mu.Unlock()
		return nil, errors.New("injected transient failure")
	}
	s.mu.Unlock()
	return s.Store.GetChunk(id)
}

func TestLoaderChunkRange(t *testing.T) {
	idx := Index{
		Chunks: []IndexChunk{
			{Start: 0, Size: 10, ID: ChunkID{0}},
			{Start: 10, Size: 10, ID: ChunkID{1}},
			{Start: 20, Size: 10, ID: ChunkID{2}},
		},
	}

	loader := newSparseFileLoader("", idx, nil)

	tests := []struct {
		// Input ranges
		start  int64
		length int64

		// Expected output (chunk positions and length)
		first int
		last  int
	}{
		{start: 0, length: 0, first: 0, last: 0},  // empty read at the start
		{start: 0, length: 1, first: 0, last: 0},  // one byte at the start
		{start: 10, length: 1, first: 1, last: 1}, // first byte in the 2nd chunk
		{start: 19, length: 1, first: 1, last: 1}, // last byte in the 2nd chunk
		{start: 0, length: 20, first: 0, last: 1}, // first two whole chunks
		{start: 5, length: 10, first: 0, last: 1}, // spanning first two chunks
		{start: 0, length: 30, first: 0, last: 2}, // whole file
		{start: 29, length: 0, first: 2, last: 2}, // empty read at the end
		{start: 29, length: 1, first: 2, last: 2}, // one byte at the end
		{start: 30, length: 1, first: 2, last: 2}, // read past the end
	}

	for _, test := range tests {
		first, chunks := loader.indexRange(test.start, test.length)
		require.Equal(t, test.first, first, "first chunk")
		require.Equal(t, test.last, chunks, "number of chunks")
	}
}

func TestSparseFileRead(t *testing.T) {
	// Sparse output file
	sparseFile, err := os.CreateTemp("", "")
	require.NoError(t, err)
	defer os.Remove(sparseFile.Name())

	// Open the store
	s, err := NewLocalStore("testdata/blob1.store", StoreOptions{})
	require.NoError(t, err)
	defer s.Close()

	// Read the index
	indexFile, err := os.Open("testdata/blob1.caibx")
	require.NoError(t, err)
	defer indexFile.Close()
	index, err := IndexFromReader(indexFile)
	require.NoError(t, err)

	// // Calculate the expected hash
	b, err := os.ReadFile("testdata/blob1")
	require.NoError(t, err)

	// Initialize the sparse file and open a handle
	sparse, err := NewSparseFile(sparseFile.Name(), index, s, SparseFileOptions{})
	require.NoError(t, err)
	h, err := sparse.Open()
	require.NoError(t, err)
	defer h.Close()

	// Read a few random ranges and compare to the expected blob content
	for range 1000 {
		length := rand.Intn(int(index.Index.ChunkSizeMax))
		offset := rand.Intn(int(index.Length()) - length - 1)

		fromSparse := make([]byte, length)
		fromBlob := make([]byte, length)

		_, err := h.ReadAt(fromSparse, int64(offset))
		require.NoError(t, err)

		_, err = bytes.NewReader(b).ReadAt(fromBlob, int64(offset))
		require.NoError(t, err)

		require.Equal(t, fromBlob, fromSparse)
	}

	// Read the whole file. After this it should match the whole blob
	whole := make([]byte, index.Length())
	_, err = h.ReadAt(whole, 0)
	require.NoError(t, err)

	blobHash := sha256.Sum256(b)
	sparseHash := sha256.Sum256(whole)
	require.Equal(t, blobHash, sparseHash)
}

// TestSparseFileRetryAfterFailedLoad ensures that a transient fetch failure
// for a chunk does not permanently poison its region of the sparse file. The
// failed load must surface an error, and a subsequent read must retry and
// return the real chunk data rather than the zeroed (never-written) region.
func TestSparseFileRetryAfterFailedLoad(t *testing.T) {
	sparseFile, err := os.CreateTemp("", "")
	require.NoError(t, err)
	defer os.Remove(sparseFile.Name())

	s, err := NewLocalStore("testdata/blob1.store", StoreOptions{})
	require.NoError(t, err)
	defer s.Close()

	indexFile, err := os.Open("testdata/blob1.caibx")
	require.NoError(t, err)
	defer indexFile.Close()
	index, err := IndexFromReader(indexFile)
	require.NoError(t, err)

	b, err := os.ReadFile("testdata/blob1")
	require.NoError(t, err)

	// Pick the first chunk that actually needs fetching from the store. Null
	// chunks are served from the truncated sparse file and never hit GetChunk.
	nullChunk := NewNullChunk(index.Index.ChunkSizeMax)
	var target IndexChunk
	found := false
	for _, c := range index.Chunks {
		if c.ID != nullChunk.ID {
			target = c
			found = true
			break
		}
	}
	require.True(t, found, "expected at least one non-null chunk")

	// Store that fails the first GetChunk for the target chunk, then works.
	flaky := &flakyStore{Store: s, failID: target.ID, failsLeft: 1}

	sparse, err := NewSparseFile(sparseFile.Name(), index, flaky, SparseFileOptions{})
	require.NoError(t, err)
	h, err := sparse.Open()
	require.NoError(t, err)
	defer h.Close()

	buf := make([]byte, target.Size)

	// First read of the target chunk's range must surface the transient error.
	_, err = h.ReadAt(buf, int64(target.Start))
	require.Error(t, err)

	// The retry must actually load the chunk and return real data, not the
	// zeroed sparse-file region.
	_, err = h.ReadAt(buf, int64(target.Start))
	require.NoError(t, err)

	expected := make([]byte, target.Size)
	_, err = bytes.NewReader(b).ReadAt(expected, int64(target.Start))
	require.NoError(t, err)
	require.Equal(t, expected, buf)
}
