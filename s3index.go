package desync

import (
	"io"

	"path"

	"net/url"

	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
	"github.com/pkg/errors"
)

// S3Store is a read-write store with S3 backing
type S3IndexStore struct {
	S3StoreBase
}

// NewS3Store creates an index store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3S3_SECRET_KEY, or via the desync config file.
func NewS3IndexStore(location *url.URL, s3Creds *credentials.Credentials, region string) (s S3IndexStore, e error) {
	b, err := NewS3StoreBase(location, s3Creds, region)
	if err != nil {
		return s, err
	}
	return S3IndexStore{b}, nil
}

// Get and Index Reader from an S3 store, returns an error if the specified index file does not exist.
func (s S3IndexStore) GetIndexReader(name string) (r io.ReadCloser, e error) {
	obj, err := s.client.GetObject(s.bucket, s.prefix+name, minio.GetObjectOptions{})
	if err != nil {
		return r, errors.Wrap(err, s.String())
	}
	return obj, nil
}

// GetIndex returns an Index structure from the store
func (s S3IndexStore) GetIndex(name string) (i Index, e error) {
	obj, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer obj.Close()
	return IndexFromReader(obj)
}

// StoreIndex writes the index file to the S3 store
func (s S3IndexStore) StoreIndex(name string, idx Index) error {
	contentType := "application/octet-stream"
	r, w := io.Pipe()

	go func() {
		defer w.Close()
		idx.WriteTo(w)
	}()

	_, err := s.client.PutObject(s.bucket, s.prefix+name, r, -1, minio.PutObjectOptions{ContentType: contentType})
	return errors.Wrap(err, path.Base(s.Location))
}
