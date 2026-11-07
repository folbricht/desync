// Package s3 implements chunk and index stores in S3-compatible object storage.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/folbricht/desync"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

var _ desync.WriteStore = Store{}

// StoreBase is the base object for all chunk and index stores with S3 backing
type StoreBase struct {
	Location   string
	client     *minio.Client
	bucket     string
	prefix     string
	opt        desync.StoreOptions
	converters desync.Converters

	// Chunk file extension, derived from the converters at construction
	extension string
}

// Store is a read-write store with S3 backing
type Store struct {
	StoreBase
}

// NewStoreBase initializes a base object used for chunk or index stores backed by S3.
func NewStoreBase(u *url.URL, s3Creds *credentials.Credentials, region string, opt desync.StoreOptions, lookupType minio.BucketLookupType) (StoreBase, error) {
	converters, err := opt.StorageConverters()
	if err != nil {
		return StoreBase{}, err
	}
	s := StoreBase{Location: u.String(), opt: opt, converters: converters, extension: converters.StorageExtension()}
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

	// Keep an idle connection for every concurrent request. With fewer, the
	// connections above minio's defaults, per host and in total, would be
	// closed and reopened for every request.
	transport, err := minio.DefaultTransport(useSSL)
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}
	transport.MaxIdleConnsPerHost = max(transport.MaxIdleConnsPerHost, opt.MaxConcurrency())
	transport.MaxIdleConns = max(transport.MaxIdleConns, opt.MaxConcurrency())

	s.client, err = minio.New(u.Host, &minio.Options{
		Creds:        s3Creds,
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookupType,
		Transport:    transport,
	})
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}
	return s, nil
}

func (s StoreBase) String() string {
	return s.Location
}

// Close the S3 base store. NOP operation but needed to implement the store interface.
func (s StoreBase) Close() error { return nil }

// NewStore creates a chunk store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3_SECRET_KEY, or via the desync config file.
func NewStore(location *url.URL, s3Creds *credentials.Credentials, region string, opt desync.StoreOptions, lookupType minio.BucketLookupType) (s Store, e error) {
	b, err := NewStoreBase(location, s3Creds, region, opt, lookupType)
	if err != nil {
		return s, err
	}
	return Store{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s Store) GetChunk(id desync.ChunkID) (*desync.Chunk, error) {
	name := s.nameFromID(id)
	var attempt int
retry:
	attempt++
	// The context bounds the whole attempt, not just the request setup: the
	// object body is read below, and GetObject only issues the request on the
	// first read. Each attempt gets its own, mirroring the per-request timeout
	// the HTTP stores get from their client.
	ctx, cancel := s.opt.ContextWithTimeout(context.Background())
	defer cancel()
	obj, err := s.client.GetObject(ctx, s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		if attempt <= s.opt.ErrorRetry {
			cancel()
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
				return nil, desync.ChunkMissing{ID: id}
			}
		}
		if attempt <= s.opt.ErrorRetry {
			_ = obj.Close()
			cancel()
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
		// Without ListBucket perms in AWS, we get Permission Denied for a missing chunk, not 404
		return nil, errors.Wrap(err, fmt.Sprintf("chunk %s could not be retrieved from s3 store", id))
	}

	// A short read of the chunk body (e.g. flaky transport/endpoint) can leave us
	// with truncated data that fails to decompress or hash. Treat that the same as
	// other transient errors and retry under the --error-retry policy.
	chunk, err := desync.NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
	if err != nil {
		if attempt <= s.opt.ErrorRetry {
			desync.Log.Info("chunk failed validation, retrying", "chunk", id, "object", name, "attempt", attempt, "error", err)
			_ = obj.Close()
			cancel()
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
		return nil, errors.Wrap(err, s.String())
	}
	return chunk, nil
}

// StoreChunk adds a new chunk to the store
func (s Store) StoreChunk(chunk *desync.Chunk) error {
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}
	var attempt int
retry:
	attempt++
	ctx, cancel := s.opt.ContextWithTimeout(context.Background())
	defer cancel()
	_, err = s.client.PutObject(ctx, s.bucket, name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		if attempt < s.opt.ErrorRetry {
			cancel()
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
	}
	return errors.Wrap(err, s.String())
}

// HasChunk returns true if the chunk is in the store
func (s Store) HasChunk(id desync.ChunkID) (bool, error) {
	name := s.nameFromID(id)
	ctx, cancel := s.opt.ContextWithTimeout(context.Background())
	defer cancel()
	_, err := s.client.StatObject(ctx, s.bucket, name, minio.StatObjectOptions{})
	return err == nil, nil
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s Store) RemoveChunk(id desync.ChunkID) error {
	name := s.nameFromID(id)
	ctx, cancel := s.opt.ContextWithTimeout(context.Background())
	defer cancel()
	return s.client.RemoveObject(ctx, s.bucket, name, minio.RemoveObjectOptions{})
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s Store) Prune(ctx context.Context, ids map[desync.ChunkID]struct{}) error {
	// The listing is bound to ctx, so cancelling it also stops the goroutine
	// feeding the channel.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	objectCh := s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true})
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return desync.Interrupted{}
		default:
		}

		id, err := desync.ChunkIDFromObjectName(object.Key, s.prefix, s.extension)
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

func (s Store) nameFromID(id desync.ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID + s.extension
	return name
}
