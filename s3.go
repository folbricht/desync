package desync

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	minio "github.com/minio/minio-go"
)

// S3Store is a read-write store with S3 backing
type S3Store struct {
	Location string
	client   *minio.Client
	bucket   string
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
		return s, fmt.Errorf("invalid scheme '%s', expected 's3+http(s)'", u.Scheme)
	}
	var useSSL bool
	if strings.HasSuffix(u.Scheme, "s") {
		useSSL = true
	}

	// Pull the bucket from a path-style URL
	s.bucket = filepath.Base(u.Path)

	// Read creds from the environment
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")

	s.client, err = minio.New(u.Host, accessKey, secretKey, useSSL)
	return s, err
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s S3Store) GetChunk(id ChunkID) ([]byte, error) {
	obj, err := s.client.GetObject(s.bucket, id.String(), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
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
	_, err := s.client.PutObject(s.bucket, id.String(), bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: contentType})
	return err
}

// HasChunk returns true if the chunk is in the store
func (s S3Store) HasChunk(id ChunkID) bool {
	_, err := s.client.StatObject(s.bucket, id.String(), minio.StatObjectOptions{})
	return err == nil
}

func (s S3Store) String() string {
	return s.Location
}

func (s S3Store) Close() error { return nil }
