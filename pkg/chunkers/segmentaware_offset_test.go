package chunkers

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Segment sizes used by the "offsettest" segmenter. It reads the whole
// input like a real self-describing format would.
var offsetTestSegments = []uint64{30000, 50000, 20000, 70000, 40000, 90000, 10000, 60000}

func init() {
	Segmenters["offsettest"] = func(r io.Reader) ([]uint64, error) {
		if _, err := io.Copy(io.Discard, r); err != nil {
			return nil, err
		}
		return offsetTestSegments, nil
	}
}

func TestSegmentAwareStartOffset(t *testing.T) {
	var size uint64
	boundaries := make(map[uint64]bool)
	for _, s := range offsetTestSegments {
		size += s
		boundaries[size] = true
	}
	data := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(data)

	dir := t.TempDir()
	name := filepath.Join(dir, "blob.offsettest")
	require.NoError(t, os.WriteFile(name, data, 0644))
	var csv bytes.Buffer
	for _, s := range offsetTestSegments {
		csv.WriteString(strconv.FormatUint(s, 10) + "\n")
	}
	csvName := filepath.Join(dir, "segments.csv")
	require.NoError(t, os.WriteFile(csvName, csv.Bytes(), 0644))

	for _, source := range []string{"self", "csv"} {
		for _, startPos := range []uint64{0, 30000, 45000, 80000, size - 10000, size - 1, size} {
			t.Run(source+"/"+strconv.FormatUint(startPos, 10), func(t *testing.T) {
				f, err := os.Open(name)
				require.NoError(t, err)
				defer f.Close()
				_, err = f.Seek(int64(startPos), io.SeekStart)
				require.NoError(t, err)

				params := DefaultChunkerParams()
				if source == "csv" {
					params.Options = "segmentsSource=" + csvName
				}
				c, err := NewChunker("segmentaware", f, params)
				require.NoError(t, err)

				// Chunks cover the file from startPos and, with only small
				// segments, end on segment boundaries.
				pos := startPos
				for {
					start, b, err := c.Next()
					require.NoError(t, err)
					if len(b) == 0 {
						break
					}
					require.Equal(t, pos-startPos, start)
					require.Equal(t, data[pos:pos+uint64(len(b))], b)
					pos += uint64(len(b))
					require.True(t, boundaries[pos], "chunk ends at %d, not on a segment boundary", pos)
				}
				require.Equal(t, size, pos)
			})
		}
	}
}

func TestSegmentAwareStartPastEnd(t *testing.T) {
	name := filepath.Join(t.TempDir(), "blob.offsettest")
	var size int64
	for _, s := range offsetTestSegments {
		size += int64(s)
	}
	require.NoError(t, os.WriteFile(name, make([]byte, size), 0644))
	f, err := os.Open(name)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Seek(size+1, io.SeekStart)
	require.NoError(t, err)

	_, err = NewChunker("segmentaware", f, DefaultChunkerParams())
	require.ErrorContains(t, err, "past the end of the file")
}
