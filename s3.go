package desync

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

// S3Store is a read-write store with S3 backing
type S3Store struct {
	Location string
	client   *minio.Client
	bucket   string
	prefix   string
}

// NewS3Store creates an instance of a chunk store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3S3_SECRET_KEY.
func NewS3Store(location string) (S3Store, error) {
	s := S3Store{Location: location}
	u, err := url.Parse(location)
	if err != nil {
		return s, err
	}
	if !strings.HasPrefix(u.Scheme, "s3+http") {
		return s, fmt.Errorf("invalid scheme '%s', expected 's3+http' or 's3+https'", u.Scheme)
	}
	var useSSL bool
	if strings.HasSuffix(u.Scheme, "s") {
		useSSL = true
	}

	// Pull the bucket as well as the prefix from a path-style URL
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return s, fmt.Errorf("expected bucket name in path of '%s'", u.Scheme)
	}
	f := strings.Split(path, "/")
	s.bucket = f[0]
	s.prefix = strings.Join(f[1:], "/")
	if s.prefix != "" {
		s.prefix += "/"
	}

	// Read creds from the environment and setup a client
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")

	s.client, err = minio.New(u.Host, accessKey, secretKey, useSSL)
	if err != nil {
		return s, errors.Wrap(err, location)
	}

	// Might as well confirm the bucket exists
	bucketExists, err := s.client.BucketExists(s.bucket)
	if err != nil {
		return s, errors.Wrap(err, location)
	}
	if !bucketExists {
		return s, fmt.Errorf("bucket '%s' does not exist in %s", s.bucket, location)
	}
	return s, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s S3Store) GetChunk(id ChunkID) ([]byte, error) {
	name := s.objectName(id)
	obj, err := s.client.GetObject(s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.StatusCode == http.StatusNotFound {
			return nil, ChunkMissing{ID: id}
		}
	}
	return b, err
}

// StoreChunk adds a new chunk to the store
func (s S3Store) StoreChunk(id ChunkID, b []byte) error {
	contentType := "application/zstd"
	name := s.objectName(id)
	_, err := s.client.PutObject(s.bucket, name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: contentType})
	return errors.Wrap(err, s.String())
}

// HasChunk returns true if the chunk is in the store
func (s S3Store) HasChunk(id ChunkID) bool {
	name := s.objectName(id)
	_, err := s.client.StatObject(s.bucket, name, minio.StatObjectOptions{})
	return err == nil
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s S3Store) RemoveChunk(id ChunkID) error {
	name := s.objectName(id)
	return s.client.RemoveObject(s.bucket, name)
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s S3Store) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.client.ListObjectsV2(s.bucket, s.prefix, false, doneCh)
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

		id, err := ChunkIDFromString(strings.TrimPrefix(object.Key, s.prefix))
		if err != nil {
			return err // TBD: Should we stop when runnning into a non-chunk object or simply continue?
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

func (s S3Store) String() string {
	return s.Location
}

func (s S3Store) Close() error { return nil }

// Upgrade converts the storage layout in S3 from the old format (just a flat
// layout) to the current layout which prefixes every chunk with the first 4
// characters of the checksum as well as a .cacnk extension. This aligns the
// layout with that of local stores and allows the used of sync tools outside
// of this tool, local stores could be copied into S3 for example.
func (s S3Store) Upgrade(ctx context.Context) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.client.ListObjectsV2(s.bucket, s.prefix, false, doneCh)
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

		// Skip if this one's already in the new format
		if strings.HasSuffix(object.Key, chunkFileExt) {
			continue
		}

		// Skip if we can't parse this checksum, must be an unrelated file
		id, err := ChunkIDFromString(strings.TrimPrefix(object.Key, s.prefix))
		if err != nil {
			continue
		}

		// Copy the chunk with the new name
		newName := s.objectName(id)
		src := minio.NewSourceInfo(s.bucket, object.Key, nil)
		dst, err := minio.NewDestinationInfo(s.bucket, newName, nil, nil)
		if err != nil {
			return err
		}
		if err = s.client.CopyObject(dst, src); err != nil {
			return err
		}

		// Once copied, drop the old chunk
		if err = s.client.RemoveObject(s.bucket, object.Key); err != nil {
			return err
		}
	}
	return nil
}

func (s S3Store) objectName(id ChunkID) string {
	sID := id.String()
	return s.prefix + sID[0:4] + "/" + sID + chunkFileExt
}
