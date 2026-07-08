package desync

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalStoreCompressed(t *testing.T) {
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunk(dataIn)
	id := chunkIn.ID()

	// Store the chunk
	err = s.StoreChunk(chunkIn)
	require.NoError(t, err)

	// Check it's in the store
	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	require.True(t, hasChunk, "chunk not found in store")

	// Pull the data the "official" way
	chunkOut, err := s.GetChunk(id)
	require.NoError(t, err)

	dataOut, err := chunkOut.Data()
	require.NoError(t, err)

	// Compare the data that went in with what came out
	require.Equal(t, dataIn, dataOut)

	// Now let's look at the file in the store directly to make sure it's compressed
	_, name := s.nameFromID(id)
	b, err := os.ReadFile(name)
	require.NoError(t, err)
	require.NotEqual(t, dataIn, b, "chunk is not compressed")
}

func TestLocalStoreUncompressed(t *testing.T) {
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{Uncompressed: true})
	require.NoError(t, err)

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunk(dataIn)
	id := chunkIn.ID()

	err = s.StoreChunk(chunkIn)
	require.NoError(t, err)

	// Check it's in the store
	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	require.True(t, hasChunk, "chunk not found in store")

	// Pull the data the "official" way
	chunkOut, err := s.GetChunk(id)
	require.NoError(t, err)

	dataOut, err := chunkOut.Data()
	require.NoError(t, err)

	// Compare the data that went in with what came out
	require.Equal(t, dataIn, dataOut)

	// Now let's look at the file in the store directly to make sure it's uncompressed
	_, name := s.nameFromID(id)
	b, err := os.ReadFile(name)
	require.NoError(t, err)

	require.Equal(t, dataIn, b, "chunk is compressed")
}

func TestLocalStoreErrorHandling(t *testing.T) {
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{})
	require.NoError(t, err)

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunk(dataIn)
	id := chunkIn.ID()
	err = s.StoreChunk(chunkIn)
	require.NoError(t, err)

	// Now put an invalid chunk into the store
	idInvalid, err := ChunkIDFromString("1000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)

	dirInvalid, nameInvalid := s.nameFromID(idInvalid)
	_ = os.Mkdir(dirInvalid, 0755)
	err = os.WriteFile(nameInvalid, []byte("invalid data"), 0644)
	require.NoError(t, err)

	// Also add a blank chunk
	idBlank, err := ChunkIDFromString("2000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)

	dirBlank, nameBlank := s.nameFromID(idBlank)
	_ = os.Mkdir(dirBlank, 0755)
	err = os.WriteFile(nameBlank, nil, 0644)
	require.NoError(t, err)

	// Let's see if we can retrieve the good chunk and get errors from the bad ones
	_, err = s.GetChunk(id)
	require.NoError(t, err)

	_, err = s.GetChunk(idInvalid)
	if _, ok := err.(ChunkInvalid); !ok {
		t.Fatal(err)
	}
	_, err = s.GetChunk(idBlank)
	if _, ok := err.(ChunkInvalid); !ok {
		t.Fatal(err)
	}

	// Run the verify with repair enabled which should get rid of the invalid and blank chunks
	err = s.Verify(context.Background(), 1, true, io.Discard)
	require.NoError(t, err)

	// Let's see if we can still retrieve the good chunk and get Not Found for the others
	_, err = s.GetChunk(id)
	require.NoError(t, err)

	_, err = s.GetChunk(idInvalid)
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal(err)
	}
	_, err = s.GetChunk(idBlank)
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal(err)
	}
}

// A read failure that isn't "file does not exist" must be reported to the
// caller, not turned into a misleading ChunkInvalid or, with SkipVerify, a
// chunk holding partial data.
func TestLocalStoreGetChunkReadError(t *testing.T) {
	store := t.TempDir()

	s, err := NewLocalStore(store, StoreOptions{SkipVerify: true})
	require.NoError(t, err)

	// Put a directory where the chunk file is expected, making os.ReadFile
	// fail with an error other than IsNotExist on all platforms
	id := NewChunk([]byte("some data")).ID()
	_, p := s.nameFromID(id)
	require.NoError(t, os.MkdirAll(p, 0755))

	_, err = s.GetChunk(id)
	require.Error(t, err)
	require.NotErrorAs(t, err, &ChunkMissing{})
	require.NotErrorAs(t, err, &ChunkInvalid{})
}

// Hex-encoded 256-bit keys used in encryption tests.
const (
	testEncryptionKey  = "6368616e676520746869732070617373776f726420746f206120736563726574"
	otherEncryptionKey = "746f74616c6c7920646966666572656e74206b65792075736564206865726521"
)

func TestLocalStoreUncompressedEncrypted(t *testing.T) {
	store := t.TempDir()

	s, err := NewLocalStore(store,
		StoreOptions{
			Uncompressed:  true,
			Encryption:    true,
			EncryptionKey: testEncryptionKey,
		},
	)
	require.NoError(t, err)

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunk(dataIn)
	id := chunkIn.ID()

	err = s.StoreChunk(chunkIn)
	require.NoError(t, err)

	// Check it's in the store
	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	require.True(t, hasChunk, "chunk not found in store")

	// Pull the data the "official" way
	chunkOut, err := s.GetChunk(id)
	require.NoError(t, err)

	dataOut, err := chunkOut.Data()
	require.NoError(t, err)

	// Compare the data that went in with what came out
	require.Equal(t, dataIn, dataOut)

	// Now let's look at the file in the store directly to make sure it's actually
	// encrypted, meaning it should not match the plain (uncompressed) text
	_, name := s.nameFromID(id)
	b, err := os.ReadFile(name)
	require.NoError(t, err)
	require.NotEqual(t, dataIn, b, "chunk is not encrypted")
}

func TestLocalStoreCompressedEncrypted(t *testing.T) {
	store := t.TempDir()

	s, err := NewLocalStore(store,
		StoreOptions{
			Uncompressed:  false,
			Encryption:    true,
			EncryptionKey: testEncryptionKey,
		},
	)
	require.NoError(t, err)

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunk(dataIn)
	id := chunkIn.ID()

	err = s.StoreChunk(chunkIn)
	require.NoError(t, err)

	// Check it's in the store
	hasChunk, err := s.HasChunk(id)
	require.NoError(t, err)
	require.True(t, hasChunk, "chunk not found in store")

	// Pull the data the "official" way
	chunkOut, err := s.GetChunk(id)
	require.NoError(t, err)

	dataOut, err := chunkOut.Data()
	require.NoError(t, err)

	// Compare the data that went in with what came out
	require.Equal(t, dataIn, dataOut)

	// Now let's look at the file in the store directly and confirm it is
	// compressed and encrypted (in that order!).
	_, name := s.nameFromID(id)
	b, err := os.ReadFile(name)
	require.NoError(t, err)

	// First decrypt it, using the correct key
	key, err := hex.DecodeString(testEncryptionKey)
	require.NoError(t, err)
	dec, err := NewXChaCha20Poly1305(key)
	require.NoError(t, err)
	decrypted, err := dec.fromStorage(b)
	require.NoError(t, err)

	// Now decompress
	decompressed, err := Decompress(nil, decrypted)
	require.NoError(t, err)

	// And it should match the original content
	require.Equal(t, dataIn, decompressed)
}

func TestLocalStoreKeyMismatch(t *testing.T) {
	store := t.TempDir()

	// Build 2 stores accessing the same files but with different keys
	s1, err := NewLocalStore(store,
		StoreOptions{
			Encryption:    true,
			EncryptionKey: testEncryptionKey,
		},
	)
	require.NoError(t, err)
	s2, err := NewLocalStore(store,
		StoreOptions{
			Encryption:    true,
			EncryptionKey: otherEncryptionKey,
		},
	)
	require.NoError(t, err)

	// Make up some data and store it using the first key
	dataIn := []byte("some data")

	chunkIn := NewChunk(dataIn)
	id := chunkIn.ID()

	err = s1.StoreChunk(chunkIn)
	require.NoError(t, err)

	// Pull the data with the right key and compare it
	chunkOut, err := s1.GetChunk(id)
	require.NoError(t, err)
	dataOut, err := chunkOut.Data()
	require.NoError(t, err)
	require.Equal(t, dataIn, dataOut)

	// Try to get the chunk with a different key, expect a not-found
	// since the chunk extensions are different for different keys.
	_, err = s2.GetChunk(id)
	require.Error(t, err)
	require.ErrorAs(t, err, &ChunkMissing{})
}
