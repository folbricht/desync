package desync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestProtocol(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	server := NewProtocol(r1, w2)
	client := NewProtocol(r2, w1)

	// Test data
	uncompressed := []byte{0, 0, 1, 1, 2, 2}
	inChunk := NewChunk(uncompressed)
	compressed, _ := Compressor{}.toStorage(uncompressed)
	cID := inChunk.ID()

	ctx, cancel := context.WithCancel(t.Context())
	g, gCtx := errgroup.WithContext(ctx)
	defer cancel()

	// Server
	g.Go(func() error {
		flags, err := client.Initialize(CaProtocolReadableStore)
		if err != nil {
			return err
		}
		if flags&CaProtocolPullChunks == 0 {
			return errors.New("client not asking for chunks")
		}
		for {
			m, err := client.ReadMessage()
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}
				return err
			}
			switch m.Type {
			case CaProtocolRequest:
				id, err := ChunkIDFromSlice(m.Body[8:40])
				if err != nil {
					return err
				}
				if err := client.SendProtocolChunk(id, 0, compressed); err != nil {
					return err
				}
			default:

				return errors.New("unexpected message")
			}
		}
	})

	// Client
	g.Go(func() error {
		defer cancel()
		flags, err := server.Initialize(CaProtocolPullChunks)
		if err != nil {
			return err
		}
		if flags&CaProtocolReadableStore == 0 {
			return errors.New("server not offering chunks")
		}

		chunk, err := server.RequestChunk(cID)
		if err != nil {
			return err
		}
		b, _ := chunk.Data()
		if !bytes.Equal(b, uncompressed) {
			return errors.New("chunk data doesn't match expected")
		}
		return nil
	})

	<-gCtx.Done()
	// unblock client/server in case of an error
	r1.Close()
	r2.Close()
	w1.Close()
	w2.Close()

	err := g.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
