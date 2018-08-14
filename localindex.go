package desync

import (
	"os"
	"strings"

	"fmt"

	"io"

	"github.com/pkg/errors"
)

// LocalStore index store
type LocalIndexStore struct {
	Path string
}

// NewLocalStore creates an instance of a local castore, it only checks presence
// of the store
func NewLocaIndexlStore(path string) (LocalIndexStore, error) {
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

// Get and Index Reader from a local store, returns an error if the specified index file does not exist.
func (s LocalIndexStore) GetIndexReader(name string) (rdr io.ReadCloser, e error) {
	return os.Open(s.Path + name)
}

// GetIndex returns an Index structure from the store
func (s LocalIndexStore) GetIndex(name string) (i Index, e error) {
	f, err := s.GetIndexReader(name)
	if err != nil {
		return i, nil
	}
	defer f.Close()
	idx, err := IndexFromReader(f)
	if os.IsNotExist(err) {
		err = errors.Errorf("Index file does not exist: %v", err)
	}
	return idx, err
}

// GetIndex returns an Index structure from the store
func (s LocalIndexStore) StoreIndex(name string, idx Index) error {
	// Write the index to file
	i, err := os.Create(s.Path + name)
	if err != nil {
		return err
	}
	defer i.Close()
	_, err = idx.WriteTo(i)
	return err
}

func (r LocalIndexStore) String() string {
	return r.Path
}

func (s LocalIndexStore) Close() error { return nil }
