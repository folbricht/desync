package desync

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelfSeed(t *testing.T) {
	// Setup a temporary store
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	// Build a number of fake chunks that can then be used in the test in any order
	type rawChunk struct {
		id   ChunkID
		data []byte
	}
	size := 1024
	numChunks := 10
	chunks := make([]rawChunk, numChunks)

	for i := range numChunks {
		b := make([]byte, size)
		rand.Read(b)
		chunk := NewChunk(b)
		require.NoError(t, s.StoreChunk(chunk))
		chunks[i] = rawChunk{chunk.ID(), b}
	}

	// Define tests with files with different content, by building files out
	// of sets of byte slices to create duplication or not between the target and
	// its seeds. Also define a min/max of bytes that should be cloned (from the
	// self-seed). That number can vary since even with 1 worker goroutine there
	// another feeder goroutine which can influence timings/results a little.
	tests := map[string]struct {
		index     []int
		minCloned int
		maxCloned int
	}{
		"single chunk": {
			index:     []int{0},
			minCloned: 0,
			maxCloned: 0,
		},
		"repeating single chunk": {
			index:     []int{0, 0, 0, 0, 0},
			minCloned: 3 * size,
			maxCloned: 4 * size,
		},
		"repeating chunk sequence": {
			index:     []int{0, 1, 2, 0, 1, 2, 2},
			minCloned: 4 * size,
			maxCloned: 4 * size,
		},
		"repeating chunk sequence mid file": {
			index:     []int{1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3},
			minCloned: 7 * size,
			maxCloned: 7 * size,
		},
		"repeating chunk sequence reversed": {
			index:     []int{0, 1, 2, 2, 1, 0},
			minCloned: 2 * size,
			maxCloned: 3 * size,
		},
		"non-repeating chunks": {
			index:     []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			minCloned: 0,
			maxCloned: 0,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Build an index from the target chunks
			var idx Index
			var b []byte
			for i, p := range test.index {
				chunk := IndexChunk{
					ID:    chunks[p].id,
					Start: uint64(i * size),
					Size:  uint64(size),
				}
				b = append(b, chunks[p].data...)
				idx.Chunks = append(idx.Chunks, chunk)
			}

			// Calculate the expected checksum
			sum := md5.Sum(b)

			// Build a temp target file to extract into
			dst := filepath.Join(t.TempDir(), "dst")

			// Extract the file
			stats, err := AssembleFile(context.Background(), dst, idx, s, nil,
				AssembleOptions{1, InvalidSeedActionBailOut},
			)
			require.NoError(t, err)

			// Compare the checksums to that of the input data
			b, err = os.ReadFile(dst)
			require.NoError(t, err)
			require.Equal(t, sum, md5.Sum(b), "checksum of extracted file doesn't match expected")

			// Compare to the expected number of bytes copied or cloned from the seed
			fromSeed := int(stats.BytesCopied + stats.BytesCloned)
			require.GreaterOrEqual(t, fromSeed, test.minCloned, "bytes copied/cloned from self-seed")
			require.LessOrEqual(t, fromSeed, test.maxCloned, "bytes copied/cloned from self-seed")
		})
	}

}
