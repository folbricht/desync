// Package gcs implements chunk and index stores in Google Cloud Storage.
package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/folbricht/desync"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

var _ desync.WriteStore = Store{}

// StoreBase is the base object for all chunk and index stores with Google
// Storage backing
type StoreBase struct {
	Location   string
	client     *storage.BucketHandle
	bucket     string
	prefix     string
	opt        desync.StoreOptions
	converters desync.Converters

	// Chunk file extension, derived from the converters at construction
	extension string
}

// Store is a read-write store with Google Storage backing
type Store struct {
	StoreBase
}

// normalizePrefix converts path to a regular format,
// where there never is a leading slash,
// and every folder name always is followed by a slash
// so example outputs will be:
//
//	<blank string>
//	folder1/
//	folder1/folder2/folder3/
func normalizePrefix(path string) string {
	prefix := strings.Trim(path, "/")

	if prefix != "" {
		prefix += "/"
	}

	return prefix
}

// NewStoreBase initializes a base object used for chunk or index stores
// backed by Google Storage.
func NewStoreBase(u *url.URL, opt desync.StoreOptions) (StoreBase, error) {
	ctx := context.TODO()
	converters, err := opt.StorageConverters()
	if err != nil {
		return StoreBase{}, err
	}
	s := StoreBase{Location: u.String(), opt: opt, converters: converters, extension: converters.StorageExtension()}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	// Pull the bucket as well as the prefix from a path-style URL
	s.bucket = u.Host
	s.prefix = normalizePrefix(u.Path)

	client, err := storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, s.String())
	}

	s.client = client.Bucket(s.bucket)
	return s, nil
}

func (s StoreBase) String() string {
	return s.Location
}

// Close the GCS base store. NOP operation but needed to implement the store interface.
func (s StoreBase) Close() error { return nil }

// NewStore creates a chunk store with Google Storage backing. The URL
// should be provided like this: gs://bucketname/prefix
// Credentials are passed in via the environment variables. TODO
func NewStore(location *url.URL, opt desync.StoreOptions) (s Store, e error) {
	b, err := NewStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return Store{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s Store) GetChunk(id desync.ChunkID) (*desync.Chunk, error) {
	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = desync.Log.With(
			"bucket", s.bucket,
			"name", name,
		)
	)

	rc, err := s.client.Object(name).NewReader(ctx)

	if errors.Is(err, storage.ErrObjectNotExist) {
		log.Warn("Unable to create reader for object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, desync.ChunkMissing{ID: id}
	} else if err != nil {
		log.Error("Unable to retrieve object from GCS bucket", "error", err)
		return nil, errors.Wrap(err, s.String())
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)

	if errors.Is(err, storage.ErrObjectNotExist) {
		log.Warn("Unable to read from object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, desync.ChunkMissing{ID: id}
	} else if err != nil {
		log.Error("Unable to retrieve object from GCS bucket", "error", err)
		return nil, errors.Wrap(err, fmt.Sprintf("chunk %s could not be retrieved from GCS bucket", id))
	}

	log.Debug("Retrieved chunk from GCS bucket")

	return desync.NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
}

// StoreChunk adds a new chunk to the store
func (s Store) StoreChunk(chunk *desync.Chunk) error {

	ctx := context.TODO()
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())

	var (
		log = desync.Log.With(
			"bucket", s.bucket,
			"name", name,
		)
	)

	b, err := chunk.Storage(s.converters)
	if err != nil {
		log.Error("Cannot retrieve chunk data", "error", err)
		return err
	}

	r := bytes.NewReader(b)
	w := s.client.Object(name).NewWriter(ctx)
	w.ContentType = contentType
	_, err = io.Copy(w, r)

	if err != nil {
		log.Error("Error when copying data from local filesystem to object in GCS bucket", "error", err)
		return errors.Wrap(err, s.String())
	}

	err = w.Close()
	if err != nil {
		log.Error("Error when finalizing copying of data from local filesystem to object in GCS bucket", "error", err)
		return errors.Wrap(err, s.String())
	}

	log.Debug("Uploaded chunk to GCS bucket")
	return nil
}

// HasChunk returns true if the chunk is in the store
func (s Store) HasChunk(id desync.ChunkID) (bool, error) {

	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = desync.Log.With(
			"bucket", s.bucket,
			"name", name,
		)
	)

	_, err := s.client.Object(name).Attrs(ctx)

	if errors.Is(err, storage.ErrObjectNotExist) {
		log.Debug("Chunk does not exist in GCS bucket", "exists", false)
		return false, nil
	} else if err != nil {
		log.Error("Unable to query attributes for object in GCS bucket", "error", err)
		return false, err
	} else {
		log.Debug("Chunk exists in GCS bucket", "exists", true)
		return true, nil
	}
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s Store) RemoveChunk(id desync.ChunkID) error {
	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = desync.Log.With(
			"bucket", s.bucket,
			"name", name,
		)
	)

	err := s.client.Object(name).Delete(ctx)

	if err != nil {
		log.Error("Unable to delete object in GCS bucket", "error", err)
		return err
	} else {
		log.Debug("Removed chunk from GCS bucket")
		return nil
	}
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s Store) Prune(ctx context.Context, ids map[desync.ChunkID]struct{}) error {
	query := &storage.Query{Prefix: s.prefix}
	it := s.client.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		id, err := desync.ChunkIDFromObjectName(attrs.Name, s.prefix, s.extension)
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
