package chunkers

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// The cut decision depends on the whole FastCDCWindowSize window: changing a
// byte 40 bytes before a boundary has to move it.
func TestFastCDCWindowCoversAllBytes(t *testing.T) {
	params := ChunkerParams{Min: 256, Avg: 1024, Max: 4096}
	data := make([]byte, 1<<20)
	rand.New(rand.NewSource(1)).Read(data)

	firstChunk := func(b []byte) int {
		c, err := NewChunker("fastcdc", bytes.NewReader(b), params)
		require.NoError(t, err)
		_, chunk, err := c.Next()
		require.NoError(t, err)
		return len(chunk)
	}

	var cuts, moved int
	for start := 0; cuts < 100; {
		l := firstChunk(data[start:])
		if l == int(params.Max) {
			start += l
			continue // no boundary found, nothing to test
		}
		cuts++
		changed := bytes.Clone(data[start : start+int(params.Max)])
		changed[l-40] ^= 0xff
		if firstChunk(changed) != l {
			moved++
		}
		start += l
	}
	require.GreaterOrEqual(t, moved, 95, "only %d of %d boundaries moved", moved, cuts)
}
