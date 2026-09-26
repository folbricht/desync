package desync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Positions before minPos aren't looked at, however often the chunk repeats
// there. Otherwise every lookup in a long stretch of one repeated chunk would
// walk all its earlier occurrences, making planning quadratic.
func TestLongestMatchFromSkipsEarlierPositions(t *testing.T) {
	const n = 10000
	var idx Index
	for i := range n {
		idx.Chunks = append(idx.Chunks, IndexChunk{ID: ChunkID{1}, Start: uint64(i) * 100, Size: 100})
	}
	s := newSeedIndex(idx, false)

	var considered int
	pos, length := s.longestMatchFrom(idx.Chunks, 9000, 9001, 100, func(p int) int {
		considered++
		return p - 9000
	})
	require.Equal(t, 9100, pos)
	require.Equal(t, 100, length)
	require.Equal(t, 100, considered, "candidates looked at")
}
