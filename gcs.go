package desync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

var _ WriteStore = GCStore{}

// GCStoreBase is the base object for all chunk and index stores with Google
// Storage backing
type GCStoreBase struct {
	Location string
	client   *storage.BucketHandle
	bucket   string
	prefix   string
	opt      StoreOptions
}

// GCStore is a read-write store with Google Storage backing
type GCStore struct {
	GCStoreBase
}

// NewGCStoreBase initializes a base object used for chunk or index stores
// backed by Google Storage.
func NewGCStoreBase(u *url.URL, opt StoreOptions) (GCStoreBase, error) {
	var err error
	ctx := context.TODO()
	s := GCStoreBase{Location: u.String(), opt: opt}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	// Pull the bucket as well as the prefix from a path-style URL
	s.bucket = u.Host
	s.prefix = u.Path

	if s.prefix != "" {
		if s.prefix[0] == '/' {
			s.prefix = s.prefix[1:]
		}
		s.prefix += "/"
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, s.String())
	}

	s.client = client.Bucket(s.bucket)
	return s, nil
}

func (s GCStoreBase) String() string {
	return s.Location
}

// Close the GCS base store. NOP opertation but needed to implement the store interface.
func (s GCStoreBase) Close() error { return nil }

// NewGCStore creates a chunk store with Google Storage backing. The URL
// should be provided like this: gs://bucketname/prefix
// Credentials are passed in via the environment variables. TODO
func NewGCStore(location *url.URL, opt StoreOptions) (s GCStore, e error) {
	b, err := NewGCStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return GCStore{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s GCStore) GetChunk(id ChunkID) (*Chunk, error) {
	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	rc, err := s.client.Object(name).NewReader(ctx)

	if err == storage.ErrObjectNotExist {
		log.Warning("Unable to create reader for object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, ChunkMissing{ID: id}
	} else if err != nil {
		log.WithError(err).Error("Unable to retrieve object from GCS bucket")
		return nil, errors.Wrap(err, s.String())
	}
	defer rc.Close()

	b, err := ioutil.ReadAll(rc)

	if err == storage.ErrObjectNotExist {
		log.Warning("Unable to read from object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, ChunkMissing{ID: id}
	} else if err != nil {
		log.WithError(err).Error("Unable to retrieve object from GCS bucket")
		return nil, errors.Wrap(err, fmt.Sprintf("chunk %s could not be retrieved from GCS bucket", id))
	}

	log.Debug("Retrieved chunk from GCS bucket")

	if s.opt.Uncompressed {
		return NewChunkWithID(id, b, nil, s.opt.SkipVerify)
	}
	return NewChunkWithID(id, nil, b, s.opt.SkipVerify)
}

// StoreChunk adds a new chunk to the store
func (s GCStore) StoreChunk(chunk *Chunk) error {

	ctx := context.TODO()
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())

	var (
		b   []byte
		err error
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	if s.opt.Uncompressed {
		b, err = chunk.Uncompressed()
	} else {
		b, err = chunk.Compressed()
	}

	if err != nil {
		log.WithError(err).Error("Cannot retrieve chunk data")
		return errors.Wrap(err, s.String())
	}

	r := bytes.NewReader(b)
	w := s.client.Object(name).NewWriter(ctx)
	w.ContentType = contentType
	_, err = io.Copy(w, r)

	if err != nil {
		log.WithError(err).Error("Error when copying data from local filesystem to object in GCS bucket")
		return errors.Wrap(err, s.String())
	}
	err = w.Close()
	if err != nil {
		log.WithError(err).Error("Error when finalizing copying of data from local filesystem to object in GCS bucket")
		return errors.Wrap(err, s.String())
	}

	log.Debug("Uploaded chunk to GCS bucket")

	return nil
}

// HasChunk returns true if the chunk is in the store
func (s GCStore) HasChunk(id ChunkID) (bool, error) {

	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	_, err := s.client.Object(name).Attrs(ctx)

	if err == storage.ErrObjectNotExist {
		log.WithField("exists", false).Debug("Chunk does not exist in GCS bucket")
		return false, nil
	} else if err != nil {
		log.WithError(err).Error("Unable to query attributes for object in GCS bucket")
		return false, err
	} else {
		log.WithField("exists", true).Debug("Chunk exists in GCS bucket")
		return true, nil
	}
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s GCStore) RemoveChunk(id ChunkID) error {
	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	err := s.client.Object(name).Delete(ctx)

	if err != nil {
		log.WithError(err).Error("Unable to delete object in GCS bucket")
		return err
	} else {
		log.Debug("Removed chunk from GCS bucket")
		return nil
	}
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s GCStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
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

		id, err := s.idFromName(attrs.Name)
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

func (s GCStore) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}

func (s GCStore) idFromName(name string) (ChunkID, error) {
	var n string
	if s.opt.Uncompressed {
		if !strings.HasSuffix(name, UncompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), UncompressedChunkExt)
	} else {
		if !strings.HasSuffix(name, CompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), CompressedChunkExt)
	}
	fragments := strings.Split(n, "/")
	if len(fragments) != 2 {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	idx := fragments[0]
	sid := fragments[1]
	if !strings.HasPrefix(sid, idx) {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	return ChunkIDFromString(sid)
}
