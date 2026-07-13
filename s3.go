package desync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var _ WriteStore = S3Store{}

// S3StoreBase is the base object for all chunk and index stores with S3 backing
type S3StoreBase struct {
	Location   string
	client     *minio.Client
	bucket     string
	prefix     string
	opt        StoreOptions
	converters Converters

	// Chunk file extension, derived from the converters at construction
	extension string
}

// S3Store is a read-write store with S3 backing
type S3Store struct {
	S3StoreBase
}

// NewS3StoreBase initializes a base object used for chunk or index stores backed by S3.
func NewS3StoreBase(u *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (S3StoreBase, error) {
	converters, err := opt.StorageConverters()
	if err != nil {
		return S3StoreBase{}, err
	}
	s := S3StoreBase{Location: u.String(), opt: opt, converters: converters, extension: converters.storageExtension()}
	if !strings.HasPrefix(u.Scheme, "s3+http") {
		return s, fmt.Errorf("invalid scheme '%s', expected 's3+http' or 's3+https'", u.Scheme)
	}
	var useSSL bool
	if strings.Contains(u.Scheme, "https") {
		useSSL = true
	}

	// Pull the bucket as well as the prefix from a path-style URL
	bPath := strings.Trim(u.Path, "/")
	if bPath == "" {
		return s, fmt.Errorf("expected bucket name in path of '%s'", u.Scheme)
	}
	f := strings.Split(bPath, "/")
	s.bucket = f[0]
	s.prefix = strings.Join(f[1:], "/")

	if s.prefix != "" {
		s.prefix += "/"
	}

	s.client, err = minio.NewWithOptions(u.Host, &minio.Options{
		Creds:        s3Creds,
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookupType,
	})
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}
	return s, nil
}

func (s S3StoreBase) String() string {
	return s.Location
}

// Close the S3 base store. NOP operation but needed to implement the store interface.
func (s S3StoreBase) Close() error { return nil }

// NewS3Store creates a chunk store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3_SECRET_KEY, or via the desync config file.
func NewS3Store(location *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (s S3Store, e error) {
	b, err := NewS3StoreBase(location, s3Creds, region, opt, lookupType)
	if err != nil {
		return s, err
	}
	return S3Store{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s S3Store) GetChunk(id ChunkID) (*Chunk, error) {
	name := s.nameFromID(id)
	var attempt int
retry:
	attempt++
	obj, err := s.client.GetObject(s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		if attempt <= s.opt.ErrorRetry {
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := io.ReadAll(obj)
	if err != nil {
		// Don't retry if the chunk or the bucket doesn't exist, those aren't
		// transient errors. A missing chunk in particular is a normal
		// occurrence when this store is behind a router or cache.
		if e, ok := err.(minio.ErrorResponse); ok {
			switch e.Code {
			case "NoSuchBucket":
				return nil, fmt.Errorf("bucket '%s' does not exist", s.bucket)
			case "NoSuchKey":
				return nil, ChunkMissing{ID: id}
			}
		}
		if attempt <= s.opt.ErrorRetry {
			obj.Close()
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
		// Without ListBucket perms in AWS, we get Permission Denied for a missing chunk, not 404
		return nil, errors.Wrap(err, fmt.Sprintf("chunk %s could not be retrieved from s3 store", id))
	}

	// A short read of the chunk body (e.g. flaky transport/endpoint) can leave us
	// with truncated data that fails to decompress or hash. Treat that the same as
	// other transient errors and retry under the --error-retry policy.
	chunk, err := NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
	if err != nil {
		if attempt <= s.opt.ErrorRetry {
			Log.WithFields(logrus.Fields{
				"chunk":   id,
				"object":  name,
				"attempt": attempt,
			}).WithError(err).Info("chunk failed validation, retrying")
			obj.Close()
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
		return nil, err
	}
	return chunk, nil
}

// StoreChunk adds a new chunk to the store
func (s S3Store) StoreChunk(chunk *Chunk) error {
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}
	var attempt int
retry:
	attempt++
	_, err = s.client.PutObject(s.bucket, name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		if attempt < s.opt.ErrorRetry {
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
	}
	return errors.Wrap(err, s.String())
}

// HasChunk returns true if the chunk is in the store
func (s S3Store) HasChunk(id ChunkID) (bool, error) {
	name := s.nameFromID(id)
	_, err := s.client.StatObject(s.bucket, name, minio.StatObjectOptions{})
	return err == nil, nil
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s S3Store) RemoveChunk(id ChunkID) error {
	name := s.nameFromID(id)
	return s.client.RemoveObject(s.bucket, name)
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s S3Store) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.client.ListObjectsV2(s.bucket, s.prefix, true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}

		id, err := chunkIDFromObjectName(object.Key, s.prefix, s.extension)
		if err != nil {
			continue
		}

		// Drop the chunk if it's not on the list
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s S3Store) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID + s.extension
	return name
}
