package chunkers

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateChunkingSettings(t *testing.T) {
	desc, err := ValidateChunkingSettings(ChunkingSettings{ChunkerName: "fastcdc", ChunkerParams: DefaultChunkerParams()})
	require.NoError(t, err)
	require.Equal(t, "fastcdc", desc.Name)

	// An empty name selects the default chunker
	desc, err = ValidateChunkingSettings(ChunkingSettings{ChunkerParams: DefaultChunkerParams()})
	require.NoError(t, err)
	require.Equal(t, DefaultChunkerName, desc.Name)

	_, err = ValidateChunkingSettings(ChunkingSettings{ChunkerName: "nope", ChunkerParams: DefaultChunkerParams()})
	require.ErrorContains(t, err, "unknown chunker 'nope'")

	_, err = ValidateChunkingSettings(ChunkingSettings{ChunkerName: "fastcdc", ChunkerParams: ChunkerParams{Min: 32, Avg: 128, Max: 512}})
	require.ErrorContains(t, err, "min chunk size too small")
}

func TestNewChunkerEmptyNameIsDefault(t *testing.T) {
	data := make([]byte, 1<<20)
	rand.New(rand.NewSource(1)).Read(data)

	chunks := func(name string) [][]byte {
		c, err := NewChunker(name, bytes.NewReader(data), DefaultChunkerParams())
		require.NoError(t, err)
		var out [][]byte
		for {
			_, b, err := c.Next()
			require.NoError(t, err)
			if len(b) == 0 {
				return out
			}
			out = append(out, bytes.Clone(b))
		}
	}
	require.Equal(t, chunks(DefaultChunkerName), chunks(""))
}

func TestSegmentAwareSubchunkerWindow(t *testing.T) {
	source := filepath.Join(t.TempDir(), "segments.csv")
	require.NoError(t, os.WriteFile(source, []byte("100\n"), 0644))

	// The min size only has to cover the subchunker's window
	params := ChunkerParams{Min: BuzhashWindowSize, Avg: 4 * BuzhashWindowSize, Max: 16 * BuzhashWindowSize,
		Options: "segmentsSource=" + source + ",subchunker=buzhash"}
	_, err := NewChunker("segmentaware", bytes.NewReader(make([]byte, 100)), params)
	require.NoError(t, err)

	params.Options = "segmentsSource=" + source + ",subchunker=fastcdc"
	_, err = NewChunker("segmentaware", bytes.NewReader(make([]byte, 100)), params)
	require.ErrorContains(t, err, "subchunker: min chunk size too small")
}
