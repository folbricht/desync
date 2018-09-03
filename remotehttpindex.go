package desync

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/url"
)

// RemoteHTTPIndex is a remote index store accessed via HTTP.
type RemoteHTTPIndex struct {
	*RemoteHTTPBase
}

// NewRemoteHTTPIndexStore initializes a new store that pulls the specified index file via HTTP(S) from
// a remote web server.
func NewRemoteHTTPIndexStore(location *url.URL, n int, cert string, key string) (*RemoteHTTPIndex, error) {
	b, err := NewRemoteHTTPStoreBase(location, n, cert, key)
	if err != nil {
		return nil, err
	}
	return &RemoteHTTPIndex{b}, nil
}

// GetIndexReader returns an index reader from an HTTP store. Fails if the specified index
// file does not exist.
func (r RemoteHTTPIndex) GetIndexReader(name string) (rdr io.ReadCloser, e error) {
	b, err := r.GetObject(name)
	if err != nil {
		return rdr, err
	}
	rc := ioutil.NopCloser(bytes.NewReader(b))
	return rc, nil
}

// GetIndex returns an Index structure from the store
func (r *RemoteHTTPIndex) GetIndex(name string) (i Index, e error) {
	ir, err := r.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	return IndexFromReader(ir)
}

// StoreIndex adds a new chunk to the store
func (r *RemoteHTTPIndex) StoreIndex(name string, idx Index) error {
	rdr, w := io.Pipe()

	go func() {
		defer w.Close()
		idx.WriteTo(w)
	}()
	return r.StoreObject(name, rdr)
}
