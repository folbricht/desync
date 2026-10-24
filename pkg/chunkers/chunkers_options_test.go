package chunkers

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkerOptionsRejected(t *testing.T) {
	for _, name := range RegisteredNames() {
		desc := FindChunkerByName(name)
		if desc.TakesOptions {
			continue
		}
		t.Run(name, func(t *testing.T) {
			params := DefaultChunkerParams()
			params.Options = "subchunker=fastcdc"
			_, err := NewChunker(name, bytes.NewReader(nil), params)
			require.ErrorContains(t, err, "doesn't take options")
		})
	}
}

func TestSegmentAwareMergeSmallSegmentsOption(t *testing.T) {
	source := filepath.Join(t.TempDir(), "segments.csv")
	require.NoError(t, os.WriteFile(source, []byte("100\n"), 0644))

	for value, merge := range map[string]bool{"true": true, "1": true, "False": false, "0": false} {
		options, err := parseSegmentAwareChunkerOptions("mergeSmallSegments=" + value)
		require.NoError(t, err)
		require.Equal(t, merge, options.mergeSmallSegments, value)
	}
	for _, value := range []string{"no", "off", "flase", ""} {
		_, err := parseSegmentAwareChunkerOptions("mergeSmallSegments=" + value)
		require.ErrorContains(t, err, "invalid mergeSmallSegments", value)
	}

	// Options still reach the chunker that takes them
	params := DefaultChunkerParams()
	params.Options = "segmentsSource=" + source + ",mergeSmallSegments=false"
	_, err := NewChunker("segmentaware", bytes.NewReader(make([]byte, 100)), params)
	require.NoError(t, err)
}
