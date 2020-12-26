package desync

import (
	"context"
	"io/ioutil"
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
	b, err := ioutil.ReadFile(name)
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
	b, err := ioutil.ReadFile(name)
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
	err = ioutil.WriteFile(nameInvalid, []byte("invalid data"), 0644)
	require.NoError(t, err)

	// Also add a blank chunk
	idBlank, err := ChunkIDFromString("2000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)

	dirBlank, nameBlank := s.nameFromID(idBlank)
	_ = os.Mkdir(dirBlank, 0755)
	err = ioutil.WriteFile(nameBlank, nil, 0644)
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
	err = s.Verify(context.Background(), 1, true, ioutil.Discard)
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
