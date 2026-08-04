package desync

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// String() and MarshalJSON() must stay on value receivers. With pointer
// receivers, a ChunkID value doesn't implement these interfaces and gets
// formatted or marshalled as a raw byte array instead of hex.
var (
	_ fmt.Stringer   = ChunkID{}
	_ json.Marshaler = ChunkID{}
)

const testChunkIDHex = "dda036cc9be7d1b1c3f6f4d3d1e2ed0f9e1f74e8b0d1a2b3c4d5e6f708192a3b"

func TestChunkIDFormatting(t *testing.T) {
	id, err := ChunkIDFromString(testChunkIDHex)
	require.NoError(t, err)

	// A value, not just a pointer, needs to render as hex
	require.Equal(t, testChunkIDHex, fmt.Sprintf("%s", id))
	require.Equal(t, testChunkIDHex, fmt.Sprintf("%v", id))
	require.Equal(t, `"`+testChunkIDHex+`"`, fmt.Sprintf("%q", id))
	require.Equal(t, testChunkIDHex, fmt.Sprintf("%s", &id))

	// Same for values carried in errors, which are printed by value
	require.Equal(t,
		fmt.Sprintf("chunk %s missing from store", testChunkIDHex),
		ChunkMissing{ID: id}.Error(),
	)
}

func TestChunkIDMarshalJSON(t *testing.T) {
	id, err := ChunkIDFromString(testChunkIDHex)
	require.NoError(t, err)

	// Map values aren't addressable, so a pointer-receiver MarshalJSON would
	// be skipped here and the ID emitted as an array of numbers
	b, err := json.Marshal(map[string]ChunkID{"id": id})
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"`+testChunkIDHex+`"}`, string(b))

	// Round-trip through a struct, as done by inspect-chunks
	b, err = json.Marshal([]ChunkAdditionalInfo{{ID: id, UncompressedSize: 1024}})
	require.NoError(t, err)
	require.JSONEq(t, `[{"id":"`+testChunkIDHex+`","uncompressed_size":1024}]`, string(b))

	var got []ChunkAdditionalInfo
	require.NoError(t, json.Unmarshal(b, &got))
	require.Equal(t, id, got[0].ID)
}
