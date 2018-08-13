package desync

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"path"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

// SFTPStore is a remote store that uses SFTP over SSH to access chunks
type SFTPStoreBase struct {
	location *url.URL
	path     string
	client   *sftp.Client
	cancel   context.CancelFunc
}

type SFTPStore struct {
	*SFTPStoreBase
}

// Creates a base sftp client
func newSFTPStoreBase(location *url.URL) (*SFTPStoreBase, error) {
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
	ctx, cancel := context.WithCancel(context.Background())
	c := exec.CommandContext(ctx, sshCmd, host, "-s", "sftp")
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
	client, err := sftp.NewClientPipe(r, w)
	if err != nil {
		return nil, err
	}
	return &SFTPStoreBase{location, path, client, cancel}, nil
}

// StoreChunk adds a new chunk to the store
func (s *SFTPStoreBase) StoreObject(name string, r io.Reader) error {
	// Write to a tempfile on the remote server. This is not 100% guaranteed to not
	// conflict between gorouties, there's no tempfile() function for remote servers.
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
			s.client.Mkdir(d)
			errCount++
			goto retry
		}
		return errors.Wrap(err, "sftp:create "+tmpfile)
	}

	if _, err := io.Copy(f, r); err != nil {
		s.client.Remove(tmpfile)
		return errors.Wrap(err, "sftp:copying chunk data to "+tmpfile)
	}
	if err = f.Close(); err != nil {
		return errors.Wrap(err, "sftp:closing "+tmpfile)
	}
	return errors.Wrap(s.client.PosixRename(tmpfile, name), "sftp:renaming "+tmpfile+" to "+name)
}

// Close terminates all client connections
func (s *SFTPStoreBase) Close() error {
	if s.cancel != nil {
		defer s.cancel()
	}
	return s.client.Close()
}

func (s *SFTPStoreBase) String() string {
	return s.location.String()
}

// NewRemoteSSHStore establishes up to n connections with a casync chunk server
func NewSFTPStore(location *url.URL) (*SFTPStore, error) {
	b, err := newSFTPStoreBase(location)
	if err != nil {
		return nil, err
	}
	return &SFTPStore{b}, nil
}

// Get a chunk from an SFTP store, returns ChunkMissing if the file does not exist
func (s *SFTPStore) GetChunk(id ChunkID) ([]byte, error) {
	name := s.nameFromID(id)
	f, err := s.client.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = ChunkMissing{id}
		}
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s *SFTPStore) RemoveChunk(id ChunkID) error {
	name := s.nameFromID(id)
	if _, err := s.client.Stat(name); err != nil {
		return ChunkMissing{id}
	}
	return s.client.Remove(name)
}

// StoreChunk adds a new chunk to the store
func (s *SFTPStore) StoreChunk(id ChunkID, b []byte) error {
	return s.StoreObject(s.nameFromID(id), bytes.NewReader(b))
}

// HasChunk returns true if the chunk is in the store
func (s *SFTPStore) HasChunk(id ChunkID) bool {
	name := s.nameFromID(id)
	_, err := s.client.Stat(name)
	return err == nil
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s *SFTPStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	walker := s.client.Walk(s.path)

	for walker.Step() {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err := walker.Err(); err != nil {
			return err
		}
		info := walker.Stat()
		if info.IsDir() { // Skip dirs
			continue
		}
		path := walker.Path()
		if !strings.HasSuffix(path, chunkFileExt) { // Skip files without chunk extension
			continue
		}
		// Convert the name into a checksum, if that fails we're probably not looking
		// at a chunk file and should skip it.
		id, err := ChunkIDFromString(strings.TrimSuffix(filepath.Base(path), ".cacnk"))
		if err != nil {
			continue
		}
		// See if the chunk we're looking at is in the list we want to keep, if not
		// remove it.
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SFTPStore) nameFromID(id ChunkID) string {
	sID := id.String()
	return s.path + sID[0:4] + "/" + sID + chunkFileExt
}
