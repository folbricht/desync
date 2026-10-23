package chunkers

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentAwareSegmentsMismatch(t *testing.T) {
	data := make([]byte, 1<<20)
	rand.New(rand.NewSource(1)).Read(data)

	tests := map[string]struct {
		segments []int
		err      string
	}{
		"exact":                          {segments: []int{100000, 2000, 500000, len(data) - 602000}},
		"short - last segment small":     {segments: []int{100000, 100000}, err: "input is longer"},
		"short - last segment big":       {segments: []int{100000, 500000}, err: "input is longer"},
		"long - last segment small":      {segments: []int{len(data) - 1000, 2000}, err: "input ended 1000 bytes into segment 1"},
		"long - last segment big":        {segments: []int{100000, 2000000}, err: "input ended 948576 bytes into segment 1"},
		"long - extra segment":           {segments: []int{len(data), 1000}, err: "input ended 0 bytes into segment 1"},
		"long - extra big segment":       {segments: []int{len(data), 500000}, err: "input ended at the start of segment 1"},
		"short - merging disabled":       {segments: []int{100000, 100000}, err: "input is longer"},
		"long - merging disabled":        {segments: []int{len(data) - 1000, 2000}, err: "input ended 1000 bytes into segment 1"},
		"exact - single segment":         {segments: []int{len(data)}},
		"exact - many small segments":    {segments: append(repeatSegment(1000, 1000), len(data)-1000*1000)},
		"short - many small segments":    {segments: repeatSegment(1000, 1000), err: "input is longer"},
		"long - past end small segments": {segments: append([]int{len(data) - 500}, repeatSegment(1000, 3)...), err: "input ended 500 bytes into segment 1"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var csv strings.Builder
			for _, s := range test.segments {
				csv.WriteString(strconv.Itoa(s) + "\n")
			}
			source := filepath.Join(t.TempDir(), "segments.csv")
			require.NoError(t, os.WriteFile(source, []byte(csv.String()), 0644))

			options := "segmentsSource=" + source
			if strings.Contains(name, "merging disabled") {
				options += ",mergeSmallSegments=false"
			}
			params := DefaultChunkerParams()
			params.Options = options
			c, err := NewChunker("segmentaware", bytes.NewReader(data), params)
			require.NoError(t, err)

			var chunked []byte
			for {
				start, b, err := c.Next()
				if test.err != "" && err != nil {
					require.ErrorContains(t, err, test.err)
					return
				}
				require.NoError(t, err)
				if len(b) == 0 {
					break
				}
				require.Equal(t, uint64(len(chunked)), start)
				chunked = append(chunked, b...)
			}
			require.Empty(t, test.err, "expected an error")
			require.Equal(t, data, chunked)
		})
	}
}

func repeatSegment(size, n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = size
	}
	return s
}
