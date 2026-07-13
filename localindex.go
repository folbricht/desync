package desync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// errInvalidIndexName is returned when an index name is not safe to use as a
// filename in a filesystem-backed index store.
var errInvalidIndexName = errors.New("invalid index name")

// validIndexName reports whether name is safe to use as an index filename in
// filesystem-backed stores. It rejects empty names, "." and "..", names
// containing a path separator (forward slash or backslash, regardless of the
// host OS so a server is protected from cross-platform payloads) and names
// that aren't local per the OS rules (e.g. Windows reserved device names or
// volume/absolute paths). This prevents path traversal from
// attacker-controlled index names, e.g. via the index server.
func validIndexName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	return filepath.IsLocal(name)
}

// LocalIndexStore is used to read/write index files on local disk
type LocalIndexStore struct {
	Path string
}

// NewLocalIndexStore creates an instance of a local index store, it only checks presence
// of the store
func NewLocalIndexStore(path string) (LocalIndexStore, error) {
	info, err := os.Stat(path)
	if err != nil {
		return LocalIndexStore{}, err
	}
	if !info.IsDir() {
		return LocalIndexStore{}, fmt.Errorf("%s is not a directory", path)
	}
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return LocalIndexStore{Path: path}, nil
}

// GetIndexReader returns a reader of an index file in the store or an error if
// the specified index file does not exist.
func (s LocalIndexStore) GetIndexReader(name string) (rdr io.ReadCloser, e error) {
	if !validIndexName(name) {
		return nil, errInvalidIndexName
	}
	return os.Open(s.Path + name)
}

// GetIndex returns an Index structure from the store
func (s LocalIndexStore) GetIndex(name string) (i Index, e error) {
	f, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer f.Close()
	idx, err := IndexFromReader(f)
	if os.IsNotExist(err) {
		err = errors.Errorf("Index file does not exist: %v", err)
	}
	return idx, err
}

// StoreIndex stores an index in the index store with the given name.
func (s LocalIndexStore) StoreIndex(name string, idx Index) error {
	if !validIndexName(name) {
		return errInvalidIndexName
	}
	// Write the index to file
	i, err := os.Create(s.Path + name)
	if err != nil {
		return err
	}
	defer i.Close()
	_, err = idx.WriteTo(i)
	return err
}

func (s LocalIndexStore) String() string {
	return s.Path
}

// Close the index store. NOP operation, needed to implement IndexStore interface
func (s LocalIndexStore) Close() error { return nil }
