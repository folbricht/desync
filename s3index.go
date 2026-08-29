package desync

import (
	"context"
	"io"

	"path"

	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

// S3IndexStore is a read-write index store with S3 backing
type S3IndexStore struct {
	S3StoreBase
}

// NewS3IndexStore creates an index store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3_SECRET_KEY, or via the desync config file.
func NewS3IndexStore(location *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (s S3IndexStore, e error) {
	if err := opt.ValidateIndexOptions(); err != nil {
		return s, err
	}
	b, err := NewS3StoreBase(location, s3Creds, region, opt, lookupType)
	if err != nil {
		return s, err
	}
	return S3IndexStore{b}, nil
}

// GetIndexReader returns a reader for an index from an S3 store. Fails if the specified index
// file does not exist.
func (s S3IndexStore) GetIndexReader(name string) (r io.ReadCloser, e error) {
	// The index is read by the caller, after this returns, so the timeout has
	// to cover the lifetime of the reader rather than just this call. Tie the
	// context to Close instead of cancelling it here.
	ctx, cancel := s.opt.contextWithTimeout(context.Background())
	obj, err := s.client.GetObject(ctx, s.bucket, s.prefix+name, minio.GetObjectOptions{})
	if err != nil {
		cancel()
		return r, errors.Wrap(err, s.String())
	}
	return cancelOnClose{ReadCloser: obj, cancel: cancel}, nil
}

// cancelOnClose releases a context when the reader it guards is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
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

	ctx, cancel := s.opt.contextWithTimeout(context.Background())
	defer cancel()

	_, err := s.client.PutObject(ctx, s.bucket, s.prefix+name, r, -1, minio.PutObjectOptions{ContentType: contentType})
	return errors.Wrap(err, path.Base(s.Location))
}
