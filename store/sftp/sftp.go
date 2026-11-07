// Package sftp implements chunk and index stores accessed with SFTP over SSH.
package sftp

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/internal/sessionpool"
	"github.com/folbricht/desync/internal/sshhost"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

var _ desync.WriteStore = &Store{}

// StoreBase is the base object for SFTP chunk and index stores.
type StoreBase struct {
	location  *url.URL
	path      string
	client    *sftp.Client
	cancel    context.CancelFunc
	opt       desync.StoreOptions
	extension string
}

// Store is a chunk store that uses SFTP over SSH.
type Store struct {
	pool       *sessionpool.Pool[*StoreBase]
	location   *url.URL
	converters desync.Converters
}

// Creates a base sftp client. The extension is the chunk file extension
// derived from the caller's converters, or empty for index stores which
// don't use converters.
func newStoreBase(location *url.URL, opt desync.StoreOptions, extension string) (*StoreBase, error) {
	sshCmd := os.Getenv("CASYNC_SSH_PATH")
	if sshCmd == "" {
		sshCmd = "ssh"
	}
	host := location.Host
	path := location.Path
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	// If a username was given in the URL, prefix the host
	if location.User != nil {
		host = location.User.Username() + "@" + location.Host
	}
	// Reject destinations that ssh would parse as command-line options.
	if err := sshhost.Validate(host); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	// "--" terminates option parsing so the destination can't be read as a flag.
	c := exec.CommandContext(ctx, sshCmd, "-s", "--", host, "sftp")
	c.Stderr = os.Stderr
	r, err := c.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	w, err := c.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err = c.Start(); err != nil {
		cancel()
		return nil, err
	}
	client, err := sftp.NewClientPipe(r, w)
	if err != nil {
		cancel()
		return nil, err
	}
	// The stat has really two jobs. Confirm that the path actually exists on the
	// server, and also make sure the handshake has happened successfully. SSH
	// may fail if multiple instances access the SSH agent concurrently.
	if _, err = client.Stat(path); err != nil {
		cancel()
		return nil, errors.Wrapf(err, "failed to stat '%s'", path)
	}
	return &StoreBase{location, path, client, cancel, opt, extension}, nil
}

// StoreObject adds a new object to a writable index or chunk store.
func (s *StoreBase) StoreObject(name string, r io.Reader) error {
	// Write to a tempfile on the remote server. This is not 100% guaranteed to not
	// conflict between goroutines, there's no tempfile() function for remote servers.
	// Use a large enough random number instead to build a tempfile
	tmpfile := name + strconv.Itoa(rand.Int())
	d := path.Dir(name)
	var errCount int
retry:
	f, err := s.client.Create(tmpfile)
	if err != nil {
		// It's possible the parent dir doesn't yet exist. Create it while ignoring
		// errors since that could be racy and fail if another goroutine does the
		// same.
		if errCount < 1 {
			_ = s.client.Mkdir(d)
			errCount++
			goto retry
		}
		return errors.Wrap(err, "sftp:create "+tmpfile)
	}

	if _, err := io.Copy(f, r); err != nil {
		_ = s.client.Remove(tmpfile)
		return errors.Wrap(err, "sftp:copying chunk data to "+tmpfile)
	}
	if err = f.Close(); err != nil {
		return errors.Wrap(err, "sftp:closing "+tmpfile)
	}
	return errors.Wrap(s.client.PosixRename(tmpfile, name), "sftp:renaming "+tmpfile+" to "+name)
}

// Close terminates all client connections
func (s *StoreBase) Close() error {
	if s.cancel != nil {
		defer s.cancel()
	}
	return s.client.Close()
}

func (s *StoreBase) String() string {
	return s.location.String()
}

// Returns the path for a chunk
func (s *StoreBase) nameFromID(id desync.ChunkID) string {
	sID := id.String()
	name := s.path + sID[0:4] + "/" + sID + s.extension
	return name
}

// NewStore initializes a chunk store using SFTP over SSH.
func NewStore(location *url.URL, opt desync.StoreOptions) (*Store, error) {
	converters, err := opt.StorageConverters()
	if err != nil {
		return nil, err
	}
	extension := converters.StorageExtension()
	pool := sessionpool.New(opt.MaxConcurrency(), func() (*StoreBase, error) {
		return newStoreBase(location, opt, extension)
	})
	// Open the first connection right away to confirm the store can be
	// reached, the others are opened when they're needed.
	c, err := pool.Get()
	if err != nil {
		return nil, err
	}
	pool.Put(c)
	return &Store{pool, location, converters}, nil
}

// GetChunk returns a chunk from an SFTP store, returns ChunkMissing if the file does not exist
func (s *Store) GetChunk(id desync.ChunkID) (*desync.Chunk, error) {
	c, err := s.pool.Get()
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(c)
	name := c.nameFromID(id)
	f, err := c.client.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = desync.ChunkMissing{ID: id}
		}
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read from %s", name)
	}
	return desync.NewChunkFromStorage(id, b, s.converters, c.opt.SkipVerify)
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s *Store) RemoveChunk(id desync.ChunkID) error {
	c, err := s.pool.Get()
	if err != nil {
		return err
	}
	defer s.pool.Put(c)
	return c.removeChunk(id)
}

// removeChunk deletes a chunk over this connection. Returns ChunkMissing if
// the chunk is not in the store.
func (s *StoreBase) removeChunk(id desync.ChunkID) error {
	name := s.nameFromID(id)
	if _, err := s.client.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return desync.ChunkMissing{ID: id}
		}
		return err
	}
	return s.client.Remove(name)
}

// StoreChunk adds a new chunk to the store
func (s *Store) StoreChunk(chunk *desync.Chunk) error {
	c, err := s.pool.Get()
	if err != nil {
		return err
	}
	defer s.pool.Put(c)
	name := c.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}

	return c.StoreObject(name, bytes.NewReader(b))
}

// HasChunk returns true if the chunk is in the store
func (s *Store) HasChunk(id desync.ChunkID) (bool, error) {
	c, err := s.pool.Get()
	if err != nil {
		return false, err
	}
	defer s.pool.Put(c)
	name := c.nameFromID(id)
	_, err = c.client.Stat(name)
	return err == nil, nil
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s *Store) Prune(ctx context.Context, ids map[desync.ChunkID]struct{}) error {
	c, err := s.pool.Get()
	if err != nil {
		return err
	}
	defer s.pool.Put(c)
	walker := c.client.Walk(c.path)

	for walker.Step() {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return desync.Interrupted{}
		default:
		}
		if err := walker.Err(); err != nil {
			return err
		}
		info := walker.Stat()
		if info.IsDir() { // Skip dirs
			continue
		}
		// Skip files that aren't chunks matching the store's extension, they
		// may use different compression or encryption settings
		id, ok := desync.ChunkIDFromFilename(filepath.Base(walker.Path()), c.extension)
		if !ok {
			continue
		}
		// See if the chunk we're looking at is in the list we want to keep, if
		// not remove it. Use the connection this iteration already holds,
		// RemoveChunk would wait for another one from the pool, forever if
		// the pool size is 1.
		if _, ok := ids[id]; !ok {
			if err := c.removeChunk(id); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close terminates all client connections
func (s *Store) Close() error {
	return s.pool.Close((*StoreBase).Close)
}

func (s *Store) String() string {
	return s.location.String()
}
