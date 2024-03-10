package desync

import (
	"bytes"
	"crypto/sha256"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	sparseFile, err := ioutil.TempFile("", "")
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
	b, err := ioutil.ReadFile("testdata/blob1")
	require.NoError(t, err)

	// Initialize the sparse file and open a handle
	sparse, err := NewSparseFile(sparseFile.Name(), index, s, SparseFileOptions{})
	require.NoError(t, err)
	h, err := sparse.Open()
	require.NoError(t, err)
	defer h.Close()

	// Read a few random ranges and compare to the expected blob content
	for i := 0; i < 1000; i++ {
		length := rand.Intn(int(index.Index.ChunkSizeMax))
		offset := rand.Intn(int(index.Length()) - length -1)
		

		fromSparse := make([]byte, length)
		fromBlob := make([]byte, length)

		_, err := h.ReadAt(fromSparse, int64(offset))
		require.NoError(t, err)

		_, err = bytes.NewReader(b).ReadAt(fromBlob, int64(offset))
		require.NoError(t, err)

		require.Equal(t, fromBlob, fromSparse)
	}

	// Read the whole file. After this is should match the whole blob
	whole := make([]byte, index.Length())
	_, err = h.ReadAt(whole, 0)
	require.NoError(t, err)

	blobHash := sha256.Sum256(b)
	sparseHash := sha256.Sum256(whole)
	require.Equal(t, blobHash, sparseHash)
}
