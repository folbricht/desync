package casync

import (
	"fmt"

	"github.com/pkg/errors"
)

// Cache is used to connect a (typically remote) store with a local store which
// functions as disk cache. Any request to the cache for a chunk will first be
// routed to the local store, and if that fails to the slower remote store.
// Any chunks retrieved from the remote store will be stored in the local one.
type Cache struct {
	s Store
	l LocalStore
}

// NewCache returns a cache router that uses a local store as cache before
// accessing a (supposedly slower) remote one.
func NewCache(s Store, l LocalStore) Cache {
	return Cache{s: s, l: l}
}

// GetChunk first asks the local store for the chunk and then the remote one.
// If we get a chunk from the remote, it's stored locally too.
func (c Cache) GetChunk(id ChunkID) ([]byte, error) {
	b, err := c.l.GetChunk(id)
	switch err.(type) {
	case nil:
		return b, nil
	case ChunkMissing:
	default:
		return nil, err
	}
	// At this point we failed to find it in the local cache. Ask the remote
	b, err = c.s.GetChunk(id)
	if err != nil {
		return nil, err
	}
	// Got the chunk. Store it in the local cache for next time
	if err = c.l.StoreChunk(id, b); err != nil {
		return nil, errors.Wrap(err, "failed to store in local cache")
	}
	return b, nil
}

func (c Cache) String() string {
	return fmt.Sprintf("store:%s with cache %s", c.s, c.l)
}
