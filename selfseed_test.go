package desync

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"io/ioutil"
	"os"
	"testing"
)

func TestSelfSeed(t *testing.T) {
	// Setup a temporary store
	store, err := ioutil.TempDir("", "store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(store)

	s, err := NewLocalStore(store, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Build a number of fake chunks that can then be used in the test in any order
	type rawChunk struct {
		id   ChunkID
		data []byte
	}
	size := 1024
	numChunks := 10
	chunks := make([]rawChunk, numChunks)

	for i := 0; i < numChunks; i++ {
		b := make([]byte, size)
		rand.Read(b)
		chunk := NewChunk(b, nil)
		if err = s.StoreChunk(chunk); err != nil {
			t.Fatal(err)
		}
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
			dst, err := ioutil.TempFile("", "dst")
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			defer os.Remove(dst.Name())

			// Extract the file
			stats, err := AssembleFile(context.Background(), dst.Name(), idx, s, nil, 1, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Compare the checksums to that of the input data
			b, err = ioutil.ReadFile(dst.Name())
			if err != nil {
				t.Fatal(err)
			}
			outSum := md5.Sum(b)
			if sum != outSum {
				t.Fatal("checksum of extracted file doesn't match expected")
			}

			// Compare to the expected number of bytes copied or cloned from the seed
			fromSeed := int(stats.BytesCopied + stats.BytesCloned)
			if fromSeed < test.minCloned {
				t.Fatalf("expected min %d bytes copied/cloned from self-seed, got %d", test.minCloned, fromSeed)
			}
			if fromSeed > test.maxCloned {
				t.Fatalf("expected max %d bytes copied/cloned from self-seed, got %d", test.maxCloned, fromSeed)
			}
		})
	}

}
