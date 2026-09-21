package desync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
			f := filepath.Join(t.TempDir(), "input")
			b := join(input...)
			require.NoError(t, os.WriteFile(f, b, 0644))

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
						f,
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
			f := filepath.Join(t.TempDir(), "input")
			require.NoError(t, os.WriteFile(f, join(input...), 0644))

			for n := 2; n <= 8; n++ {
				t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
					index, stats, err := IndexFromFile(
						context.Background(),
						f,
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

// TestIndexFromFileDigestFlag verifies that when chunking a catar, the index
// takes the archive's feature flags but records the digest in use, not the
// digest flag stored in the archive.
func TestIndexFromFileDigestFlag(t *testing.T) {
	tests := map[string]struct {
		digest HashAlgorithm
		flag   uint64
	}{
		"sha512-256": {SHA512256{}, CaFormatSHA512256},
		"sha256":     {SHA256{}, 0},
	}

	// A catar written by desync carries CaFormatSHA512256 in its entry
	var archive bytes.Buffer
	enc := NewFormatEncoder(&archive)
	_, err := enc.Encode(FormatEntry{
		FormatHeader: FormatHeader{Size: 64, Type: CaFormatEntry},
		FeatureFlags: TarFeatureFlags,
		Mode:         os.ModeDir | 0755,
		MTime:        time.Unix(0, 0),
	})
	require.NoError(t, err)
	rnd := make([]byte, 4*ChunkSizeMaxDefault)
	rand.Read(rnd)
	archive.Write(rnd)
	f := filepath.Join(t.TempDir(), "input.catar")
	require.NoError(t, os.WriteFile(f, archive.Bytes(), 0644))

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			withDigest(t, test.digest)

			index, _, err := IndexFromFile(
				context.Background(),
				f,
				2,
				ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault,
				NewProgressBar(""),
			)
			require.NoError(t, err)
			assert.Equal(t, TarFeatureFlags&^CaFormatSHA512256|test.flag, index.Index.FeatureFlags)

			// The index must pass the digest check when read back
			var buf bytes.Buffer
			_, err = index.WriteTo(&buf)
			require.NoError(t, err)
			_, err = IndexFromReader(&buf)
			require.NoError(t, err)
		})
	}
}
