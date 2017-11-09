package casync

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"

	"github.com/pkg/errors"
)

// RemoteSSH is a remote casync store accessed via SSH. Supports running
// multiple sessions to improve throughput.
type RemoteSSH struct {
	pool chan Session // use a buffered channel as session "pool"
}

// NewRemoteSSHStore establishes up to n connections with a casync chunk server
func NewRemoteSSHStore(location *url.URL, n int) (*RemoteSSH, error) {
	remote := RemoteSSH{pool: make(chan Session, n)}
	// Start n sessions and put them into the pool (buffered channel)
	for i := 0; i < n; i++ {
		s, err := StartSession(location.Host, location.Path)
		if err != nil {
			return &remote, errors.Wrap(err, "failed to start casync session")
		}
		remote.pool <- s
	}
	return &remote, nil
}

// GetChunk requests a chunk from the server and returns a (compressed) one.
// It uses any of the n sessions this store maintains in its pool. Blocks until
// one session becomes available
func (s *RemoteSSH) GetChunk(id ChunkID) ([]byte, error) {
	session := <-s.pool
	b, err := session.RequestChunk(id)
	s.pool <- session
	return b, err
}

// Session represents one instance of a local SSH client and a remote casync
// store server. TODO: There's currently no way to stop a session, we just rely
// on the process terminating and cleaning up everything.
type Session struct {
	c *exec.Cmd
	r io.ReadCloser
	w io.WriteCloser
}

// StartSession initiates a connection to the remote store server using
// the value in CASYNC_SSH_PATH (default "ssh"), and executes the command in
// CASYNC_REMOTE_PATH (default "casync"). It then performs the HELLO handshake
// to initialze the connection before returning a Session object that can then
// be used to request chunks.
func StartSession(host string, path string) (Session, error) {
	sshCmd := os.Getenv("CASYNC_SSH_PATH")
	if sshCmd == "" {
		sshCmd = "ssh"
	}
	remoteCmd := os.Getenv("CASYNC_REMOTE_PATH")
	if remoteCmd == "" {
		remoteCmd = "casync"
	}

	c := exec.Command(sshCmd, host, fmt.Sprintf("%s pull - - - '%s'", remoteCmd, path))
	c.Stderr = os.Stderr
	r, err := c.StdoutPipe()
	if err != nil {
		return Session{}, err
	}
	w, err := c.StdinPipe()
	if err != nil {
		return Session{}, err
	}
	if err = c.Start(); err != nil {
		return Session{}, err
	}
	s := Session{c: c, r: r, w: w}
	err = s.init()
	return s, err
}

// Exchange HELLOs and make sure the other side offers the expected service
func (s Session) init() error {
	if err := s.SendHello(CaProtocolPullChunks); err != nil {
		return err
	}
	flags, err := s.RecvHello()
	if err != nil {
		return err
	}
	if flags&CaProtocolReadableStore == 0 {
		return errors.New("server does not offer chunks")
	}
	return nil
}

// ReadMessage reads a generic message from the server, verifies the length,
// extracts the type and returns the message body as byte slice
func (s Session) ReadMessage() (Message, error) {
	r := reader{s.r}

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
func (s Session) WriteMessage(m Message) error {
	len := 16 + len(m.Body)
	h := make([]byte, 16)
	binary.LittleEndian.PutUint64(h[0:8], uint64(len))
	binary.LittleEndian.PutUint64(h[8:16], uint64(m.Type))
	r := io.MultiReader(bytes.NewReader(h), bytes.NewReader(m.Body))
	_, err := io.Copy(s.w, r)
	return err
}

type Message struct {
	Type uint64
	Body []byte
}

// SendHello sends a HELLO message to the server, with the flags signaling which
// service is being requested from it.
func (s Session) SendHello(flags uint64) error {
	f := make([]byte, 8)
	binary.LittleEndian.PutUint64(f, flags)
	m := Message{Type: CaProtocolHello, Body: f}
	return s.WriteMessage(m)
}

// RecvHello waits for the server to send a HELLO, fails if anything else is
// received. Returns the flags provided by the server.
func (s Session) RecvHello() (uint64, error) {
	m, err := s.ReadMessage()
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

func (s Session) SendProtocolRequest(id ChunkID, flags uint64) error {
	// prepare the body
	b := make([]byte, 40)

	// write the flags into it
	binary.LittleEndian.PutUint64(b[0:8], flags)

	// and the chunk id
	copy(b[8:], id[:])

	m := Message{Type: CaProtocolRequest, Body: b}
	return s.WriteMessage(m)
}

// RequestChunk sends a request for a specific chunk to the server, waits for
// the response and returns the bytes in the chunk. Returns an error if the
// server reports the chunk as missing
func (s Session) RequestChunk(id ChunkID) ([]byte, error) {
	if err := s.SendProtocolRequest(id, CaProtocolRequestHighPriority); err != nil {
		return nil, err
	}
	m, err := s.ReadMessage()
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
