package desync

import (
	"bytes"
	"context"
	"crypto/sha512"
	"io"
	"testing"
)

func TestProtocolServer(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	server := NewProtocol(r1, w2)

	// Test data
	uncompressed := []byte{4, 3, 2, 1}
	compressed, _ := Compress(uncompressed)
	id := ChunkID(sha512.Sum512_256(uncompressed))
	store := TestStore{
		id: compressed,
	}
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
	b, _ := chunk.Uncompressed()
	if !bytes.Equal(b, uncompressed) {
		t.Fatal("chunk data doesn't match expected")
	}

	// This one's missing
	_, err = server.RequestChunk(ChunkID{0})
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatal("expectec ChunkMissing error, got:", err)
	}
}
