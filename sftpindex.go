package desync

import (
	"net/url"
	"os"

	"io"

	"github.com/pkg/errors"
)

type SFTPIndexStore struct {
	*SFTPStoreBase
}

// NewSFTPIndexStore establishes up to n connections with a casync index server
func NewSFTPIndexStore(location *url.URL) (*SFTPIndexStore, error) {
	b, err := newSFTPStoreBase(location)
	if err != nil {
		return nil, err
	}
	return &SFTPIndexStore{b}, nil
}

// Get and Index Reader from  an SFTP store, returns an error if the specified index file does not exist.
func (s *SFTPIndexStore) GetIndexReader(name string) (r io.ReadCloser, e error) {
	f, err := s.client.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Errorf("Index file does not exist: %v", err)
		}
		return r, err
	}
	return f, nil
}

// Get and Index from  an SFTP store, returns an error if the specified index file does not exist.
func (s *SFTPIndexStore) GetIndex(name string) (i Index, e error) {
	f, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer f.Close()
	return IndexFromReader(f)
}

// StoreChunk adds a new chunk to the store
func (s *SFTPStore) StoreIndex(name string, idx Index) error {
	r, w := io.Pipe()

	go func() {
		defer w.Close()
		idx.WriteTo(w)
	}()
	return s.StoreObject(name, r)
}
