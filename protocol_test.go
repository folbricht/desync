package desync

import (
	"bytes"
	"io"
	"testing"
)

func TestProtocol(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	server := NewProtocol(r1, w2)
	client := NewProtocol(r2, w1)

	// Test data
	uncompressed := []byte{0, 0, 1, 1, 2, 2}
	inChunk := NewChunk(uncompressed, nil)
	compressed, _ := inChunk.Compressed()
	cID := inChunk.ID()

	// Server
	go func() {
		flags, err := client.Initialize(CaProtocolReadableStore)
		if err != nil {
			t.Fatal(err)
		}
		if flags&CaProtocolPullChunks == 0 {
			t.Fatalf("client not asking for chunks")
		}
		for {
			m, err := client.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			switch m.Type {
			case CaProtocolRequest:
				id, err := ChunkIDFromSlice(m.Body[8:40])
				if err != nil {
					t.Fatal(err)
				}
				if err := client.SendProtocolChunk(id, 0, compressed); err != nil {
					t.Fatal(err)
				}
			default:

				t.Fatal("unexpected message")
			}
		}
	}()

	// Client
	flags, err := server.Initialize(CaProtocolPullChunks)
	if err != nil {
		t.Fatal(err)
	}
	if flags&CaProtocolReadableStore == 0 {
		t.Fatalf("server not offering chunks")
	}

	chunk, err := server.RequestChunk(cID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := chunk.Uncompressed()
	if !bytes.Equal(b, uncompressed) {
		t.Fatal("chunk data doesn't match expected")
	}
}
