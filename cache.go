package desync

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
)

// Cache is used to connect a (typically remote) store with a local store which
// functions as disk cache. Any request to the cache for a chunk will first be
// routed to the local store, and if that fails to the slower remote store.
// Any chunks retrieved from the remote store will be stored in the local one.
type Cache struct {
	s Store
	l WriteStore
}

// NewCache returns a cache router that uses a local store as cache before
// accessing a (supposedly slower) remote one.
func NewCache(s Store, l WriteStore) Cache {
	return Cache{s: s, l: l}
}

// GetChunk first asks the local store for the chunk and then the remote one.
// If we get a chunk from the remote, it's stored locally too.
func (c Cache) GetChunk(id ChunkID) (*Chunk, error) {
	log.Printf("Looking for %s in local cache", id)
	chunk, err := c.l.GetChunk(id)
	switch err.(type) {
	case nil:
		log.Printf("Found %s in local cache", id)
		return chunk, nil
	case ChunkMissing:
		log.Printf("Not found %s in local cache", id)
	default:
		return chunk, err
	}
	// At this point we failed to find it in the local cache. Ask the remote
	log.Printf("Requesting %s from upstream store", id)
	chunk, err = c.s.GetChunk(id)
	if err != nil {
		log.Printf("Failed request %s from upstream store : %v", id, err)
		return chunk, err
	}
	log.Printf("Storing %s from upstream store in local cache", id)
	// Got the chunk. Store it in the local cache for next time
	if err = c.l.StoreChunk(chunk); err != nil {
		return chunk, errors.Wrap(err, "failed to store in local cache")
	}
	log.Printf("Done storing %s from upstream store in local cache", id)
	return chunk, nil
}

// HasChunk first checks the cache for the chunk, then the store.
func (c Cache) HasChunk(id ChunkID) (bool, error) {
	log.Printf("Testing local cache for %s", id)
	if hasChunk, err := c.l.HasChunk(id); err != nil || hasChunk {
		if err != nil {
			log.Printf("Failed testing local cache for %s : %v", id, err)
		} else {
			log.Printf("Found %s in local cache", id)
		}
		return hasChunk, err
	}
	log.Printf("Not found %s in local cache, sending to upstream", id)
	return c.s.HasChunk(id)
}

func (c Cache) String() string {
	return fmt.Sprintf("store:%s with cache %s", c.s, c.l)
}

// Close the underlying writable chunk store
func (c Cache) Close() error {
	c.l.Close()
	return c.s.Close()
}
