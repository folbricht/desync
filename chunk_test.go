package desync

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewChunkFromStorage(t *testing.T) {
	conv := Converters{Compressor{}}
	plain := []byte("the quick brown fox jumps over the lazy dog")
	id := ChunkID(Digest.Sum(plain))
	storage, err := conv.toStorage(plain)
	require.NoError(t, err)

	t.Run("valid", func(t *testing.T) {
		c, err := NewChunkFromStorage(id, storage, conv, false)
		require.NoError(t, err)
		require.Equal(t, id, c.ID())
	})

	t.Run("undecodable storage data", func(t *testing.T) {
		// Truncated/garbage storage data fails to decompress. This must surface as
		// ChunkInvalid (so RepairableCache and 'verify --repair' still handle it) but
		// must also carry the underlying decode error rather than reporting a bogus
		// "does not match its hash 0000..." mismatch.
		_, err := NewChunkFromStorage(id, []byte("not a valid zstd stream"), conv, false)
		var ci ChunkInvalid
		require.ErrorAs(t, err, &ci)
		require.Error(t, ci.Err, "expected ChunkInvalid.Err to carry the underlying decode error")
		require.Error(t, errors.Unwrap(err), "expected the error to unwrap to the underlying decode error")
	})

	t.Run("hash mismatch", func(t *testing.T) {
		var wrongID ChunkID
		wrongID[0] = 0x01
		_, err := NewChunkFromStorage(wrongID, storage, conv, false)
		var ci ChunkInvalid
		require.ErrorAs(t, err, &ci)
		require.NoError(t, ci.Err, "expected ChunkInvalid.Err to be nil for a plain hash mismatch")
		require.Equal(t, id, ci.Sum)
	})

	t.Run("skip verify", func(t *testing.T) {
		// With skipVerify even undecodable data is accepted (no verification done).
		_, err := NewChunkFromStorage(id, []byte("garbage"), conv, true)
		require.NoError(t, err)
	})
}
