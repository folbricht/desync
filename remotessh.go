package desync

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"

	"github.com/pkg/errors"
)

var _ Store = &RemoteSSH{}

// RemoteSSH is a remote casync store accessed via SSH. Supports running
// multiple sessions to improve throughput.
type RemoteSSH struct {
	location *url.URL
	pool     chan *Protocol // use a buffered channel as session "pool"
	n        int
}

// NewRemoteSSHStore establishes up to n connections with a casync chunk server
func NewRemoteSSHStore(location *url.URL, opt StoreOptions) (*RemoteSSH, error) {
	remote := RemoteSSH{location: location, pool: make(chan *Protocol, opt.N), n: opt.N}
	// Start n sessions and put them into the pool (buffered channel)
	for i := 0; i < remote.n; i++ {
		s, err := StartProtocol(location)
		if err != nil {
			return &remote, errors.Wrap(err, "failed to start chunk server command")
		}
		remote.pool <- s
	}
	return &remote, nil
}

// GetChunk requests a chunk from the server and returns a (compressed) one.
// It uses any of the n sessions this store maintains in its pool. Blocks until
// one session becomes available
func (r *RemoteSSH) GetChunk(id ChunkID) (*Chunk, error) {
	client := <-r.pool
	chunk, err := client.RequestChunk(id)
	r.pool <- client
	return chunk, err
}

// Close terminates all client connections
func (r *RemoteSSH) Close() error {
	var err error
	for i := 0; i < r.n; i++ {
		client := <-r.pool
		err = client.SendGoodbye()
	}
	return err
}

// HasChunk returns true if the chunk is in the store. TODO: Implementing it
// this way, pulling the whole chunk just to see if it's present, is very
// inefficient. I'm not aware of a way to implement it with the casync protocol
// any other way.
func (r *RemoteSSH) HasChunk(id ChunkID) (bool, error) {
	if _, err := r.GetChunk(id); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RemoteSSH) String() string {
	return r.location.String()
}

// StartProtocol initiates a connection to the remote store server using
// the value in CASYNC_SSH_PATH (default "ssh"), and executes the command in
// CASYNC_REMOTE_PATH (default "casync"). It then performs the HELLO handshake
// to initialze the connection
func StartProtocol(u *url.URL) (*Protocol, error) {
	sshCmd := os.Getenv("CASYNC_SSH_PATH")
	if sshCmd == "" {
		sshCmd = "ssh"
	}
	remoteCmd := os.Getenv("CASYNC_REMOTE_PATH")
	if remoteCmd == "" {
		remoteCmd = "casync"
	}

	host := u.Host
	path := u.Path
	// If a username was given in the URL, prefix the host
	if u.User != nil {
		host = u.User.Username() + "@" + u.Host
	}

	c := exec.Command(sshCmd, host, fmt.Sprintf("%s pull - - - '%s'", remoteCmd, path))
	c.Stderr = os.Stderr
	r, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	w, err := c.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err = c.Start(); err != nil {
		return nil, err
	}

	// Perform the handshake with the server
	p := NewProtocol(r, w)
	flags, err := p.Initialize(CaProtocolPullChunks)
	if err != nil {
		return nil, err
	}
	if flags&CaProtocolReadableStore == 0 {
		return nil, errors.New("server not offering chunks")
	}
	return p, nil
}
