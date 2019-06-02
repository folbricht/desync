package desync

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"
)

func TestLocalStoreCompressed(t *testing.T) {
	// Setup a temporary store
	store, err := ioutil.TempDir("", "store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(store)

	s, err := NewLocalStore(store, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunkFromUncompressed(dataIn)
	id := chunkIn.ID()
	if err := s.StoreChunk(chunkIn); err != nil {
		t.Fatal(err)
	}

	// Check it's in the store
	hasChunk, err := s.HasChunk(id)
	if err != nil {
		t.Fatal((err))
	}
	if !hasChunk {
		t.Fatal("chunk not found in store")
	}

	// Pull the data the "official" way
	chunkOut, err := s.GetChunk(id)
	if err != nil {
		t.Fatal(err)
	}
	dataOut, err := chunkOut.Uncompressed()
	if err != nil {
		t.Fatal(err)
	}

	// Compare the data that went in with what came out
	if !bytes.Equal(dataIn, dataOut) {
		t.Fatal("input and output data doesn't match after store/retrieve")
	}

	// Now let's look at the file in the store directly to make sure it's compressed
	_, name := s.nameFromID(id)
	b, err := ioutil.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(dataIn, b) {
		t.Fatal("chunk is not compressed")
	}
}

func TestLocalStoreUncompressed(t *testing.T) {
	// Setup a temporary store
	store, err := ioutil.TempDir("", "store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(store)

	s, err := NewLocalStore(store, StoreOptions{Uncompressed: true})
	if err != nil {
		t.Fatal(err)
	}

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunkFromUncompressed(dataIn)
	id := chunkIn.ID()
	if err := s.StoreChunk(chunkIn); err != nil {
		t.Fatal(err)
	}

	// Check it's in the store
	hasChunk, err := s.HasChunk(id)
	if err != nil {
		t.Fatal((err))
	}
	if !hasChunk {
		t.Fatal("chunk not found in store")
	}

	// Pull the data the "official" way
	chunkOut, err := s.GetChunk(id)
	if err != nil {
		t.Fatal(err)
	}
	dataOut, err := chunkOut.Uncompressed()
	if err != nil {
		t.Fatal(err)
	}

	// Compare the data that went in with what came out
	if !bytes.Equal(dataIn, dataOut) {
		t.Fatal("input and output data doesn't match after store/retrieve")
	}

	// Now let's look at the file in the store directly to make sure it's uncompressed
	_, name := s.nameFromID(id)
	b, err := ioutil.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dataIn, b) {
		t.Fatal("chunk is not compressed")
	}
}

func TestLocalStoreErrorHandling(t *testing.T) {
	// Setup a temporary store
	store, err := ioutil.TempDir("", "store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(store)

	s, err := NewLocalStore(store, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Make up some data and store it
	dataIn := []byte("some data")

	chunkIn := NewChunkFromUncompressed(dataIn)
	id := chunkIn.ID()
	if err := s.StoreChunk(chunkIn); err != nil {
		t.Fatal(err)
	}

	// Now put an invalid chunk into the store
	idInvalid, err := ChunkIDFromString("1000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	dirInvalid, nameInvalid := s.nameFromID(idInvalid)
	os.Mkdir(dirInvalid, 0755)
	if err := ioutil.WriteFile(nameInvalid, []byte("invalid data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Also add a blank chunk
	idBlank, err := ChunkIDFromString("2000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	dirBlank, nameBlank := s.nameFromID(idBlank)
	os.Mkdir(dirBlank, 0755)
	if err := ioutil.WriteFile(nameBlank, nil, 0644); err != nil {
		t.Fatal(err)
	}

	// Let's see if we can retrieve the good chunk and get errors from the bad ones
	if _, err := s.GetChunk(id); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetChunk(idInvalid)
	if _, ok := err.(ChunkInvalid); !ok {
		t.Fatal(err)
	}
	_, err = s.GetChunk(idBlank)
	if _, ok := err.(ChunkInvalid); !ok {
		t.Fatal(err)
	}

	// Run the verify with repair enabled which should get rid of the invalid and blank chunks
	if err := s.Verify(context.Background(), 1, true, ioutil.Discard); err != nil {
		t.Fatal(err)
	}

	// Let's see if we can still retrieve the good chunk and get Not Found for the others
	if _, err := s.GetChunk(id); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetChunk(idInvalid)
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal(err)
	}
	_, err = s.GetChunk(idBlank)
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal(err)
	}
}
