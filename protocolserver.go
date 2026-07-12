package desync

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// ProtocolServer serves up chunks from a local store using the casync protocol
type ProtocolServer struct {
	p     *Protocol
	store Store
}

// NewProtocolServer returns an initialized server that can serve chunks from
// a chunk store via the casync protocol
func NewProtocolServer(r io.Reader, w io.Writer, s Store) *ProtocolServer {
	return &ProtocolServer{
		p:     NewProtocol(r, w),
		store: s,
	}
}

// Serve starts the protocol server. Blocks unless an error is encountered
func (s *ProtocolServer) Serve(ctx context.Context) error {
	flags, err := s.p.Initialize(CaProtocolReadableStore)
	if err != nil {
		return errors.Wrap(err, "failed to perform protocol handshake")
	}
	if flags&CaProtocolPullChunks == 0 {
		return fmt.Errorf("client is not requesting chunks, provided flags %x", flags)
	}
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		m, err := s.p.ReadMessage()
		if err != nil {
			return errors.Wrap(err, "failed to read protocol message from client")
		}
		switch m.Type {
		case CaProtocolRequest:
			if len(m.Body) < 40 {
				return errors.New("protocol request too small")
			}
			id, err := ChunkIDFromSlice(m.Body[8:40])
			if err != nil {
				return errors.Wrap(err, "unable to decode requested chunk id")
			}
			chunk, err := s.store.GetChunk(id)
			if err != nil {
				if _, ok := err.(ChunkMissing); !ok {
					return errors.Wrap(err, "unable to read chunk from store")
				}
				if err = s.p.SendMissing(id); err != nil {
					return errors.Wrap(err, "failed to send to client")
				}
				continue
			}
			b, err := chunk.Storage([]converter{Compressor{}})
			if err != nil {
				// The chunk is in the store but can't be converted to the
				// wire format, perhaps corrupt or encrypted with a different
				// key. Report it missing so the client can deal with the one
				// chunk instead of tearing down the whole session.
				Log.WithField("chunk", id.String()).WithError(err).Error("unable to convert chunk to wire format")
				if err = s.p.SendMissing(id); err != nil {
					return errors.Wrap(err, "failed to send to client")
				}
				continue
			}
			if err := s.p.SendProtocolChunk(chunk.ID(), CaProtocolChunkCompressed, b); err != nil {
				return errors.Wrap(err, "failed to send chunk data")
			}
		case CaProtocolAbort:
			return errors.New("client aborted connection")
		case CaProtocolGoodbye:
			return nil
		default:
			return fmt.Errorf("unexpected command (%x) from client", m.Type)
		}
	}
}
