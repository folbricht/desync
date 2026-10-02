package chunkers

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type chunkerTestFn func(t *testing.T, chunkerName string)

func runTestForChunkers(t *testing.T, testFn chunkerTestFn) {
	for _, chunkerName := range RegisteredNames() {
		if chunkerName == "segmentaware" {
			// TODO: Skip it for now, as we have no segment data for test cases
			continue
		}
		t.Run(chunkerName, func(t *testing.T) { testFn(t, chunkerName) })
	}
}

func testChunkEmptyFile(t *testing.T, chunkerName string) {
	r := bytes.NewReader([]byte{})
	c, err := NewChunker(chunkerName, r, DefaultChunkerParams())
	require.NoError(t, err)
	start, buf, err := c.Next()
	require.NoError(t, err)
	require.Empty(t, buf)
	require.Equal(t, uint64(0), start)
}

func TestChunkEmptyFile(t *testing.T) {
	runTestForChunkers(t, testChunkEmptyFile)
}

func testChunkSmallFile(t *testing.T, chunkerName string) {
	b := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	r := bytes.NewReader(b)
	c, err := NewChunker(chunkerName, r, DefaultChunkerParams())
	require.NoError(t, err)

	start, buf, err := c.Next()
	require.NoError(t, err)
	require.Len(t, buf, len(b))
	require.Equal(t, uint64(0), start)
}

func TestChunkSmallFile(t *testing.T) {
	runTestForChunkers(t, testChunkSmallFile)
}

// There are no chunk boundaries when all data is nil, make sure we get the
// max chunk size
func testChunkNoBoundary(t *testing.T, chunkerName string) {
	b := make([]byte, 1024*1024)
	r := bytes.NewReader(b)
	c, err := NewChunker(chunkerName, r, DefaultChunkerParams())
	require.NoError(t, err)
	for {
		start, buf, err := c.Next()
		require.NoError(t, err)
		if len(buf) == 0 {
			break
		}
		require.Equal(t, DefaultChunkSizeMax, uint64(len(buf)))
		require.Zero(t, start%DefaultChunkSizeMax, "unexpected start position %d", start)
	}
}

func TestChunkNoBoundary(t *testing.T) {
	runTestForChunkers(t, testChunkNoBoundary)
}

// Test with exactly min, avg, max chunk size of data
func testChunkBounds(t *testing.T, chunkerName string) {
	for _, c := range []struct {
		name string
		size uint64
	}{
		{"chunker with exactly min chunk size data", DefaultChunkSizeMin},
		{"chunker with exactly avg chunk size data", DefaultChunkSizeAvg},
		{"chunker with exactly max chunk size data", DefaultChunkSizeMax},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := make([]byte, c.size)
			r := bytes.NewReader(b)
			c, err := NewChunker(chunkerName, r, DefaultChunkerParams())
			require.NoError(t, err)

			start, buf, err := c.Next()
			require.NoError(t, err)
			require.Len(t, buf, len(b))
			require.Equal(t, uint64(0), start)
		})
	}
}

func TestChunkBounds(t *testing.T) {
	runTestForChunkers(t, testChunkBounds)
}

// Test to confirm advancing through the input without producing chunks works.
func testChunkerAdvance(t *testing.T, chunkerName string) {
	// Build an input slice that is NullChunk + <dataA> + Nullchunk + <dataB>.
	// Then skip over the data slices and we should be left with only Null chunks.
	dataA := make([]byte, 128) // Short slice
	for i := range dataA {
		dataA[i] = 'a'
	}

	dataB := make([]byte, 12*DefaultChunkSizeMax) // Long slice to ensure we read past the chunker-internal buffer
	for i := range dataB {
		dataB[i] = 'b'
	}

	//nullChunk := NewNullChunk(DefaultChunkSizeMax)
	nullChunk := make([]byte, DefaultChunkSizeMax)

	// Build the input slice consisting of Null+dataA+Null+dataB
	input := join(nullChunk, dataA, nullChunk, dataB)

	c, err := NewChunker(chunkerName, bytes.NewReader(input), DefaultChunkerParams())
	require.NoError(t, err)

	// Chunk the first part, this should be a null chunk
	_, buf, err := c.Next()
	require.NoError(t, err)
	require.Equal(t, nullChunk, buf, "expected null chunk")

	// Now skip the dataA slice
	require.NoError(t, c.Advance(len(dataA)))

	// Read the 2nd null chunk
	_, buf, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, nullChunk, buf, "expected null chunk")

	// Skip over dataB
	require.NoError(t, c.Advance(len(dataB)))

	// Should be at the end, nothing more to chunk
	_, buf, err = c.Next()
	require.NoError(t, err)
	require.Empty(t, buf, "expected end of input")
}

func TestChunkerAdvance(t *testing.T) {
	runTestForChunkers(t, testChunkerAdvance)
}

func join(slices ...[]byte) []byte {
	var out []byte
	for _, b := range slices {
		out = append(out, b...)
	}
	return out
}

type chunkerBenchFn func(b *testing.B, chunkerName string)

func runBenchForChunkers(b *testing.B, benchFn chunkerBenchFn) {
	for _, chunkerName := range RegisteredNames() {
		if chunkerName == "segmentaware" {
			// TODO: Skip it for now, as we have no segment data for test cases
			continue
		}
		b.Run(chunkerName, func(b *testing.B) { benchFn(b, chunkerName) })
	}
}

func benchmarkChunker(b *testing.B, chunkerName string) {
	data, err := os.ReadFile("../../testdata/chunker.input")
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		c, err := NewChunker(chunkerName, bytes.NewReader(data), DefaultChunkerParams())
		if err != nil {
			b.Fatal(err)
		}
		for {
			start, buf, err := c.Next()
			if err != nil {
				b.Fatal(err)
			}
			if len(buf) == 0 {
				break
			}
			chunkStart = start
			chunkBuf = buf
		}
	}
}

func BenchmarkChunkers(b *testing.B) {
	runBenchForChunkers(b, benchmarkChunker)
}

func benchmarkChunkNull(b *testing.B, chunkerName string, size int) {
	in := make([]byte, size)
	b.SetBytes(int64(size))
	for b.Loop() {
		c, err := NewChunker(chunkerName, bytes.NewReader(in), DefaultChunkerParams())
		if err != nil {
			b.Fatal(err)
		}
		for {
			start, buf, err := c.Next()
			if err != nil {
				b.Fatal(err)
			}
			if len(buf) == 0 {
				break
			}
			chunkStart = start
			chunkBuf = buf
		}
	}
}

func BenchmarkChunkNull(b *testing.B) {
	scales := []int{1, 10, 50, 100}
	for _, scale := range scales {
		b.Run(fmt.Sprintf("%vM", scale), func(b *testing.B) {
			runBenchForChunkers(b, func(b *testing.B, chunkerName string) {
				benchmarkChunkNull(b, chunkerName, scale*1024*1024)
			})

		})
	}
}
