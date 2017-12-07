package desync

import (
	"bytes"
	"io"
	"testing"
)

func TestProtocolServer(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	server := NewProtocol(r1, w2)
	// client := NewProtocol(r2, w1)

	store := TestStore{
		ChunkID{1}: []byte{4, 3, 2, 1},
	}
	ps := NewProtocolServer(r2, w1, store)

	go ps.Serve()

	// Client
	flags, err := server.Initialize(CaProtocolPullChunks)
	if err != nil {
		t.Fatal(err)
	}
	if flags&CaProtocolReadableStore == 0 {
		t.Fatalf("server not offering chunks")
	}

	// Should find this chunk
	chunk, err := server.RequestChunk(ChunkID{1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk, store[ChunkID{1}]) {
		t.Fatal("chunk data doesn't match expected")
	}

	// This one's missing
	_, err = server.RequestChunk(ChunkID{0})
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal("expectec ChunkMissing error, got:", err)
	}
}
