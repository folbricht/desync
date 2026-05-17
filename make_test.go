package desync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"os"
	"testing"

	"github.com/folbricht/tempfile"
	"github.com/stretchr/testify/require"
)

func TestParallelChunking(t *testing.T) {
	null := make([]byte, 4*ChunkSizeMaxDefault)
	rand1 := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rand1)
	rand2 := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rand2)

	tests := map[string][][]byte{
		"random input":    {rand1, rand2, rand1, rand2, rand1},
		"leading null":    {null, null, null, null, rand1, rand2},
		"trailing null":   {rand1, rand2, null, null, null, null},
		"middle null":     {rand1, null, null, null, null, rand2},
		"spread out null": {rand1, null, null, null, rand1, null, null, null, rand2},
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			// Put the input data into a file for chunking
			f, err := tempfile.New("", "")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(f.Name())
			b := join(input...)
			if _, err := f.Write(b); err != nil {
				t.Fatal(err)
			}
			f.Close()

			// Chunk the file single stream first to use the results as reference for
			// the parallel chunking
			c, err := NewChunker(bytes.NewReader(b), ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault)
			if err != nil {
				t.Fatal(err)
			}
			var expected []IndexChunk
			for {
				start, buf, err := c.Next()
				if err != nil {
					t.Fatal(err)
				}
				if len(buf) == 0 {
					break
				}
				id := ChunkID(sha512.Sum512_256(buf))
				expected = append(expected, IndexChunk{Start: start, Size: uint64(len(buf)), ID: id})
			}

			// Chunk the file with the parallel chunking algorithm and different degrees of concurrency
			for n := 1; n <= 10; n++ {
				t.Run(fmt.Sprintf("%s, n=%d", name, n), func(t *testing.T) {
					index, _, err := IndexFromFile(
						context.Background(),
						f.Name(),
						n,
						ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
						NewProgressBar(""),
					)
					if err != nil {
						t.Fatal(err)
					}

					for i := range expected {
						if expected[i] != index.Chunks[i] {
							t.Fatal("chunks from parallel splitter don't match single stream chunks")
						}
					}
				})
			}
		})
	}
}

// TestIndexFromFileStats exercises the parallel chunker's early-EOF/straggler
// path (null-heavy inputs, multiple workers) and asserts the returned
// ChunkingStats. It guards the data race where IndexFromFile copied stats by
// value while worker goroutines were still atomically updating them: with the
// join in place the counters must be complete and deterministic for every
// worker count. Run under -race to catch a regression of the join.
func TestIndexFromFileStats(t *testing.T) {
	null := make([]byte, 4*ChunkSizeMaxDefault)
	rnd := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rnd)

	tests := map[string][][]byte{
		"trailing null":   {rnd, rnd, null, null, null, null},
		"middle null":     {rnd, null, null, null, null, rnd},
		"spread out null": {rnd, null, null, null, rnd, null, null, null, rnd},
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			f, err := tempfile.New("", "")
			require.NoError(t, err)
			defer os.Remove(f.Name())
			_, err = f.Write(join(input...))
			require.NoError(t, err)
			f.Close()

			for n := 2; n <= 8; n++ {
				t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
					index, stats, err := IndexFromFile(
						context.Background(),
						f.Name(),
						n,
						ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
						NewProgressBar(""),
					)
					require.NoError(t, err)
					require.Equal(t, uint64(len(index.Chunks)), stats.ChunksAccepted,
						"ChunksAccepted should equal len(index.Chunks)")
					require.GreaterOrEqual(t, stats.ChunksProduced, stats.ChunksAccepted,
						"ChunksProduced should be >= ChunksAccepted")
					require.NotZero(t, stats.ChunksProduced,
						"workers should have produced chunks")
				})
			}
		})
	}
}
