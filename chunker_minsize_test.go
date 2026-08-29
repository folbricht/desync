package desync

import (
	"bytes"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// casync tests the rolling hash window that ends exactly at the min chunk size
// before rolling any further byte, so a boundary there produces a chunk of
// exactly min bytes. desync used to skip that test, making min+1 the smallest
// chunk it could emit and causing the two to disagree whenever a boundary fell
// on min. See the comment in Chunker.Next.
func TestChunkerBoundaryAtMinSize(t *testing.T) {
	const (
		min = 64
		avg = 1024
		max = 4096
	)
	disc := discriminatorFromAvg(avg)

	// Find a window whose hash lands on a boundary. The chunker seeds its hash
	// over the ChunkerWindowSize bytes immediately before min, so those are the
	// bytes that decide whether there is a boundary at exactly min.
	rng := rand.New(rand.NewSource(1))
	window := make([]byte, ChunkerWindowSize)
	var found bool
	for i := 0; i < 1e6 && !found; i++ {
		rng.Read(window)
		var h uint32
		for j, b := range window {
			h ^= bits.RotateLeft32(hashTable[b], ChunkerWindowSize-j-1)
		}
		found = h%disc == disc-1
	}
	require.True(t, found, "no boundary window found, cannot exercise the min-size case")

	// The window has to sit immediately before the min offset, with arbitrary
	// data before and after it.
	in := make([]byte, max*2)
	rng.Read(in)
	copy(in[min-ChunkerWindowSize:min], window)

	c, err := NewChunker(bytes.NewReader(in), min, avg, max)
	require.NoError(t, err)

	_, chunk, err := c.Next()
	require.NoError(t, err)
	require.Len(t, chunk, min, "a boundary at the min chunk size must produce a chunk of exactly min bytes")
}
