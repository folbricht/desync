package desync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Chunks converted between storage formats that share converter layers must
// only apply the difference, not rebuild the whole stack from plain data.
// Verified by using storage data that isn't valid zstd: any attempt to
// undo the shared compression layer would fail.
func TestChunkStorageSharedLayers(t *testing.T) {
	enc, err := NewXChaCha20Poly1305(testKey(t, testEncryptionKey))
	require.NoError(t, err)

	compressed := []byte("not valid zstd data")

	// Chunk from a compressed store, requested compressed+encrypted, as when
	// a chunk-server with --encryption reads from a compressed upstream store.
	// Only the encryption layer should be applied.
	c, err := NewChunkFromStorage(ChunkID{}, compressed, Converters{Compressor{}}, true)
	require.NoError(t, err)
	encrypted, err := c.Storage(Converters{Compressor{}, enc})
	require.NoError(t, err)
	decrypted, err := enc.fromStorage(encrypted)
	require.NoError(t, err)
	require.Equal(t, compressed, decrypted)

	// The reverse: a compressed+encrypted chunk written to a compressed store.
	// Only the encryption layer should be removed.
	c, err = NewChunkFromStorage(ChunkID{}, encrypted, Converters{Compressor{}, enc}, true)
	require.NoError(t, err)
	storage, err := c.Storage(Converters{Compressor{}})
	require.NoError(t, err)
	require.Equal(t, compressed, storage)

	// Same modifiers should return the storage data as-is.
	storage, err = c.Storage(Converters{Compressor{}, enc})
	require.NoError(t, err)
	require.Equal(t, encrypted, storage)
}
