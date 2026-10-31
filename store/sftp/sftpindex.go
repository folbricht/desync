package sftp

import (
	"net/url"
	"os"
	"path"

	"io"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
)

// IndexStore is an index store backed by SFTP over SSH
type IndexStore struct {
	*StoreBase
}

// NewIndexStore initializes and index store backed by SFTP over SSH.
func NewIndexStore(location *url.URL, opt desync.StoreOptions) (*IndexStore, error) {
	if err := opt.ValidateIndexOptions(); err != nil {
		return nil, err
	}
	b, err := newStoreBase(location, opt, "")
	if err != nil {
		return nil, err
	}
	return &IndexStore{b}, nil
}

// GetIndexReader returns a reader of an index from an SFTP store. Fails if the specified
// index file does not exist.
func (s *IndexStore) GetIndexReader(name string) (r io.ReadCloser, e error) {
	f, err := s.client.Open(s.pathFromName(name))
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Errorf("Index file does not exist: %v", err)
		}
		return r, err
	}
	return f, nil
}

// GetIndex reads an index from an SFTP store, returns an error if the specified index file does not exist.
func (s *IndexStore) GetIndex(name string) (i desync.Index, e error) {
	f, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer f.Close()
	return desync.IndexFromReader(f)
}

// StoreIndex adds a new index to the store
func (s *IndexStore) StoreIndex(name string, idx desync.Index) error {
	r, w := io.Pipe()

	go func() {
		_, err := idx.WriteTo(w)
		w.CloseWithError(err)
	}()
	return s.StoreObject(s.pathFromName(name), r)
}

func (s *IndexStore) pathFromName(name string) string {
	return path.Join(s.path, name)
}
