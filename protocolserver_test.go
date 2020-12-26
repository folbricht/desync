package desync

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestProtocolServer(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	server := NewProtocol(r1, w2)

	// Test data
	uncompressed := []byte{4, 3, 2, 1}
	chunkIn := NewChunk(uncompressed)
	id := chunkIn.ID()
	store := &TestStore{}
	store.StoreChunk(chunkIn)

	ps := NewProtocolServer(r2, w1, store)

	go ps.Serve(context.Background())

	// Client
	flags, err := server.Initialize(CaProtocolPullChunks)
	if err != nil {
		t.Fatal(err)
	}
	if flags&CaProtocolReadableStore == 0 {
		t.Fatalf("server not offering chunks")
	}

	// Should find this chunk
	chunk, err := server.RequestChunk(id)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := chunk.Data()
	if !bytes.Equal(b, uncompressed) {
		t.Fatal("chunk data doesn't match expected")
	}

	// This one's missing
	_, err = server.RequestChunk(ChunkID{0})
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal("expected ChunkMissing error, got:", err)
	}
}
