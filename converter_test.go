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

	// Key rotation: convert between two encrypted formats that share the
	// compression layer, as when re-encrypting a store with a new key.
	// Only the encryption layer should be swapped.
	otherEnc, err := NewXChaCha20Poly1305(testKey(t, otherEncryptionKey))
	require.NoError(t, err)
	reEncrypted, err := c.Storage(Converters{Compressor{}, otherEnc})
	require.NoError(t, err)
	decrypted, err = otherEnc.fromStorage(reEncrypted)
	require.NoError(t, err)
	require.Equal(t, compressed, decrypted)
}

// Converting a chunk to plain storage format goes through Data() which
// caches the result on the chunk, so that later consumers, such as reading
// a chunk back after storing it in an uncompressed cache, don't convert
// the same data again.
func TestChunkStoragePlainCached(t *testing.T) {
	plain := []byte("some data")
	compressed, err := Compress(plain)
	require.NoError(t, err)

	c, err := NewChunkFromStorage(ChunkID{}, compressed, Converters{Compressor{}}, true)
	require.NoError(t, err)

	storage, err := c.Storage(nil)
	require.NoError(t, err)
	require.Equal(t, plain, storage)
	require.Equal(t, plain, c.data, "plain data not cached on the chunk")
}
