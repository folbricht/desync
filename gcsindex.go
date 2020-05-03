package desync

import (
	"context"
	"io"
	"net/url"
	"path"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   s.prefix + name,
		})
	)

	obj, err := s.client.Object(s.prefix + name).NewReader(ctx)

	if err == storage.ErrObjectNotExist {
		log.Warning("Unable to create reader for object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, errors.Wrap(err, s.String())
	} else if err != nil {
		log.WithError(err).Error("Error when creating index reader from GCS bucket")
		return nil, errors.Wrap(err, s.String())
	}

	log.Debug("Created index reader from GCS bucket")
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

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   s.prefix + name,
		})
	)

	w := s.client.Object(s.prefix + name).NewWriter(ctx)
	w.ContentType = "application/octet-stream"

	_, err := idx.WriteTo(w)

	if err != nil {
		log.WithError(err).Error("Error when copying data from local filesystem to object in GCS bucket")
		w.Close()
		return errors.Wrap(err, path.Base(s.Location))
	}

	err = w.Close()

	if err != nil {
		log.WithError(err).Error("Error when finalizing copying of data from local filesystem to object in GCS bucket")
		return errors.Wrap(err, path.Base(s.Location))
	}

	log.Debug("Index written to GCS bucket")
	return nil
}
