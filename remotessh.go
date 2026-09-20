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
	pool     *sessionPool[*Protocol]
}

// NewRemoteSSHStore establishes up to n connections with a casync chunk server
func NewRemoteSSHStore(location *url.URL, opt StoreOptions) (*RemoteSSH, error) {
	// The casync protocol always exchanges compressed chunks, storage
	// options don't apply here. Reject encryption settings rather than
	// silently ignoring them.
	if opt.EncryptionConfigured() {
		return nil, errors.New("encryption is not supported by casync protocol (ssh://) stores")
	}
	remote := RemoteSSH{
		location: location,
		pool: newSessionPool(opt.maxConcurrency(), func() (*Protocol, error) {
			s, err := StartProtocol(location)
			return s, errors.Wrap(err, "failed to start chunk server command")
		}),
	}
	// Start the first session right away to confirm the store can be
	// reached, the others are started when they're needed.
	s, err := remote.pool.get()
	if err != nil {
		return &remote, err
	}
	remote.pool.put(s)
	return &remote, nil
}

// GetChunk requests a chunk from the server and returns a (compressed) one.
// It uses any of the sessions this store maintains in its pool, starting a new
// one if all are busy. Blocks until one session becomes available if the pool
// is at its limit.
func (r *RemoteSSH) GetChunk(id ChunkID) (*Chunk, error) {
	client, err := r.pool.get()
	if err != nil {
		return nil, err
	}
	defer r.pool.put(client)
	return client.RequestChunk(id)
}

// Close terminates all client connections
func (r *RemoteSSH) Close() error {
	return r.pool.close((*Protocol).SendGoodbye)
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
// to initialize the connection
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
	// Reject destinations that ssh would parse as command-line options.
	if err := validateSSHHost(host); err != nil {
		return nil, err
	}

	// "--" terminates ssh's option parsing so the destination can't be read as a
	// flag, and the path is shell-quoted so it can't break out of the remote
	// command string.
	c := exec.Command(sshCmd, "--", host, fmt.Sprintf("%s pull - - - %s", remoteCmd, shellQuote(path)))
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
