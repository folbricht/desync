package chunkers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentAwareMergedSegmentsInvariants(t *testing.T) {
	// Broken internal state is reported as an error rather than a panic
	c := &SegmentAwareChunker{}
	_, _, err := c.nextFromMergedSegments()
	require.ErrorContains(t, err, "no merged segments")

	c = &SegmentAwareChunker{mergedSegmentsSize: 1, mergedSegmentsCount: 2, nextSegmentIdx: 1}
	_, _, err = c.nextFromMergedSegments()
	require.ErrorContains(t, err, "2 merged segments, but only 1 segments read")
}

func TestSegmentAwareSmallSegmentBreakpoint(t *testing.T) {
	// Values of 4 GiB and above don't wrap around
	options, err := parseSegmentAwareChunkerOptions("smallSegmentBreakpoint=4194305")
	require.NoError(t, err)
	require.Equal(t, uint64(4194305*1024), options.smallSegmentBreakpoint)

	_, err = parseSegmentAwareChunkerOptions("smallSegmentBreakpoint=18014398509481984")
	require.ErrorContains(t, err, "invalid smallSegmentBreakpoint")
}
