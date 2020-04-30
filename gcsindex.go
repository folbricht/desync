package desync

import (
	"context"
	"io"
	"net/url"
	"path"

	"github.com/pkg/errors"
)

// GCIndexStore is a read-write index store with Google Storage backing
type GCIndexStore struct {
	GCStoreBase
}

// NewGCIndexStore creates an index store with Google Storage backing. The URL
// should be provided like this: gc://bucket/prefix
func NewGCIndexStore(location *url.URL, opt StoreOptions) (s GCIndexStore, e error) {
	b, err := NewGCStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return GCIndexStore{b}, nil
}

// GetIndexReader returns a reader for an index from an Google Storage store. Fails if the specified index
// file does not exist.
func (s GCIndexStore) GetIndexReader(name string) (r io.ReadCloser, err error) {
	ctx := context.TODO()
	obj, err := s.client.Object(s.prefix + name).NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}
	return obj, nil
}

// GetIndex returns an Index structure from the store
func (s GCIndexStore) GetIndex(name string) (i Index, e error) {
	obj, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer obj.Close()
	return IndexFromReader(obj)
}

// StoreIndex writes the index file to the Google Storage store
func (s GCIndexStore) StoreIndex(name string, idx Index) error {
	ctx := context.TODO()
	w := s.client.Object(s.prefix + name).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	_, err := idx.WriteTo(w)
	if err != nil {
		w.Close()
		return errors.Wrap(err, path.Base(s.Location))
	}
	err = w.Close()
	return errors.Wrap(err, path.Base(s.Location))
}
