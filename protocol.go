package desync

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Protocol handles the casync protocol when using remote stores via SSH
type Protocol struct {
	r           io.Reader
	w           io.Writer
	initialized bool
}

// Message represents a command sent to, or received from the communication partner.
type Message struct {
	Type uint64
	Body []byte
}

// NewProtocol creates a new casync protocol handler
func NewProtocol(r io.Reader, w io.Writer) *Protocol {
	return &Protocol{r: r, w: w}
}

// Initialize exchanges HELLOs with the other side to start a protocol session.
// Returns the (capability) flags provided by the other party.
func (p *Protocol) Initialize(flags uint64) (uint64, error) {
	var (
		wg               sync.WaitGroup
		sendErr, recvErr error
		outFlags         uint64
	)
	wg.Add(2)
	go func() { sendErr = p.SendHello(flags); wg.Done() }()
	go func() { outFlags, recvErr = p.RecvHello(); wg.Done() }()
	wg.Wait()
	if sendErr != nil {
		return 0, sendErr
	}
	if recvErr != nil {
		return 0, recvErr
	}
	p.initialized = true
	return outFlags, nil
}

// SendHello sends a HELLO message to the server, with the flags signaling which
// service is being requested from it.
func (p *Protocol) SendHello(flags uint64) error {
	f := make([]byte, 8)
	binary.LittleEndian.PutUint64(f, flags)
	m := Message{Type: CaProtocolHello, Body: f}
	return p.WriteMessage(m)
}

// RecvHello waits for the server to send a HELLO, fails if anything else is
// received. Returns the flags provided by the server.
func (p *Protocol) RecvHello() (uint64, error) {
	m, err := p.ReadMessage()
	if err != nil {
		return 0, err
	}
	if m.Type != CaProtocolHello {
		return 0, fmt.Errorf("expected protocl hello, got %x", m.Type)
	}
	if len(m.Body) != 8 {
		return 0, fmt.Errorf("unexpected length of hello msg, got %d, expected 8", len(m.Body))
	}
	return binary.LittleEndian.Uint64(m.Body), nil
}

// SendProtocolRequest requests a chunk from a server
func (p *Protocol) SendProtocolRequest(id ChunkID, flags uint64) error {
	if !p.initialized {
		return errors.New("protocol not initialized")
	}
	// prepare the body
	b := make([]byte, 40)

	// write the flags into it
	binary.LittleEndian.PutUint64(b[0:8], flags)

	// and the chunk id
	copy(b[8:], id[:])

	m := Message{Type: CaProtocolRequest, Body: b}
	return p.WriteMessage(m)
}

// SendProtocolChunk responds to a request with the content of a chunk
func (p *Protocol) SendProtocolChunk(id ChunkID, flags uint64, chunk []byte) error {
	if !p.initialized {
		return errors.New("protocol not initialized")
	}
	// prepare the body
	b := make([]byte, len(chunk)+40)

	// write the flags into it
	binary.LittleEndian.PutUint64(b[0:8], flags)

	// then the chunk id
	copy(b[8:], id[:])

	// then the chunk itself
	copy(b[40:], chunk)

	m := Message{Type: CaProtocolChunk, Body: b}
	return p.WriteMessage(m)
}

// SendMissing tells the client that the requested chunk is not available
func (p *Protocol) SendMissing(id ChunkID) error {
	if !p.initialized {
		return errors.New("protocol not initialized")
	}
	m := Message{Type: CaProtocolMissing, Body: id[:]}
	return p.WriteMessage(m)
}

// SendGoodbye tells the other side to terminate gracefully
func (p *Protocol) SendGoodbye() error {
	if !p.initialized {
		return errors.New("protocol not initialized")
	}
	m := Message{Type: CaProtocolGoodbye, Body: nil}
	return p.WriteMessage(m)
}

// RequestChunk sends a request for a specific chunk to the server, waits for
// the response and returns the bytes in the chunk. Returns an error if the
// server reports the chunk as missing
func (p *Protocol) RequestChunk(id ChunkID) ([]byte, error) {
	if !p.initialized {
		return nil, errors.New("protocol not initialized")
	}
	if err := p.SendProtocolRequest(id, CaProtocolRequestHighPriority); err != nil {
		return nil, err
	}
	m, err := p.ReadMessage()
	if err != nil {
		return nil, err
	}
	switch m.Type { // TODO: deal with ABORT messages
	case CaProtocolMissing:
		return nil, ChunkMissing{id}
	case CaProtocolChunk:
		// The body comes with flags... do we need them? Ignore for now
		if len(m.Body) < 40 {
			return nil, errors.New("received chunk too small")
		}
		cid, err := ChunkIDFromSlice(m.Body[8:40])
		if err != nil {
			return nil, err
		}
		if cid != id {
			return nil, fmt.Errorf("requested chunk %s from server, but got %s", id, cid)
		}
		// The rest should be the chunk data
		return m.Body[40:], nil
	default:
		return nil, fmt.Errorf("unexpected protocol message type %x", m.Type)
	}
}

// ReadMessage reads a generic message from the other end, verifies the length,
// extracts the type and returns the message body as byte slice
func (p *Protocol) ReadMessage() (Message, error) {
	r := reader{p.r}

	// Get the length of the message
	len, err := r.ReadUint64()
	if err != nil {
		return Message{}, err
	}

	// Got to have at least a type following the length
	if len < 16 {
		return Message{}, errors.New("message length too short")
	}

	// Read the remaining message body
	b, err := r.ReadN(len - 8)
	if err != nil {
		return Message{}, err
	}

	// Get the message type and strip it off the remaining message data
	typ := binary.LittleEndian.Uint64(b[0:8])
	b = b[8:]

	return Message{Type: typ, Body: b}, nil
}

// WriteMessage sends a generic message to the server
func (p *Protocol) WriteMessage(m Message) error {
	len := 16 + len(m.Body)
	h := make([]byte, 16)
	binary.LittleEndian.PutUint64(h[0:8], uint64(len))
	binary.LittleEndian.PutUint64(h[8:16], uint64(m.Type))
	r := io.MultiReader(bytes.NewReader(h), bytes.NewReader(m.Body))
	_, err := io.Copy(p.w, r)
	return err
}
