package gcs

import (
	"context"
	"io"
	"net/url"
	"path"

	"cloud.google.com/go/storage"
	"github.com/folbricht/desync"
	"github.com/pkg/errors"
)

// IndexStore is a read-write index store with Google Storage backing
type IndexStore struct {
	StoreBase
}

// NewIndexStore creates an index store with Google Storage backing. The URL
// should be provided like this: gc://bucket/prefix
func NewIndexStore(location *url.URL, opt desync.StoreOptions) (s IndexStore, e error) {
	if err := opt.ValidateIndexOptions(); err != nil {
		return s, err
	}
	b, err := NewStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return IndexStore{b}, nil
}

// GetIndexReader returns a reader for an index from an Google Storage store. Fails if the specified index
// file does not exist.
func (s IndexStore) GetIndexReader(name string) (r io.ReadCloser, err error) {
	ctx := context.TODO()

	var (
		log = desync.Log.With(
			"bucket", s.bucket,
			"name", s.prefix+name,
		)
	)

	obj, err := s.client.Object(s.prefix + name).NewReader(ctx)

	if err == storage.ErrObjectNotExist {
		log.Warn("Unable to create reader for object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, errors.Wrap(err, s.String())
	} else if err != nil {
		log.Error("Error when creating index reader from GCS bucket", "error", err)
		return nil, errors.Wrap(err, s.String())
	}

	log.Debug("Created index reader from GCS bucket")
	return obj, nil
}

// GetIndex returns an Index structure from the store
func (s IndexStore) GetIndex(name string) (i desync.Index, e error) {
	obj, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer obj.Close()
	return desync.IndexFromReader(obj)
}

// StoreIndex writes the index file to the Google Storage store
func (s IndexStore) StoreIndex(name string, idx desync.Index) error {
	ctx := context.TODO()

	var (
		log = desync.Log.With(
			"bucket", s.bucket,
			"name", s.prefix+name,
		)
	)

	w := s.client.Object(s.prefix + name).NewWriter(ctx)
	w.ContentType = "application/octet-stream"

	_, err := idx.WriteTo(w)

	if err != nil {
		log.Error("Error when copying data from local filesystem to object in GCS bucket", "error", err)
		_ = w.Close()
		return errors.Wrap(err, path.Base(s.Location))
	}

	err = w.Close()

	if err != nil {
		log.Error("Error when finalizing copying of data from local filesystem to object in GCS bucket", "error", err)
		return errors.Wrap(err, path.Base(s.Location))
	}

	log.Debug("Index written to GCS bucket")
	return nil
}
