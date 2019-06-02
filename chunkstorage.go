package desync

import (
	"sync"
)

// ChunkStorage stores chunks in a writable store. It can be safely used by multiple goroutines and
// contains an internal cache of what chunks have been store previously.
type ChunkStorage struct {
	sync.Mutex
	ws        WriteStore
	processed map[ChunkID]struct{}
}

// NewChunkStorage initializes a ChunkStorage object.
func NewChunkStorage(ws WriteStore) *ChunkStorage {
	s := &ChunkStorage{
		ws:        ws,
		processed: make(map[ChunkID]struct{}),
	}
	return s
}

// Mark a chunk in the in-memory cache as having been processed and returns true
// if it was already marked, and is therefore presumably already stored.
func (s *ChunkStorage) markProcessed(id ChunkID) bool {
	s.Lock()
	defer s.Unlock()
	_, ok := s.processed[id]
	s.processed[id] = struct{}{}
	return ok
}

// Unmark a chunk in the in-memory cache. This is used if a chunk is first
// marked as processed, but then actually fails to be stored. Unmarking the
// makes it eligible to be re-tried again in case of errors.
func (s *ChunkStorage) unmarkProcessed(id ChunkID) {
	s.Lock()
	defer s.Unlock()
	delete(s.processed, id)
}

// StoreChunk stores a single chunk in a synchronous manner.
func (s *ChunkStorage) StoreChunk(chunk *Chunk) (err error) {

	// Mark this chunk as done so no other goroutine will attempt to store it
	// at the same time. If this is the first time this chunk is marked, it'll
	// return false and we need to continue processing/storing the chunk below.
	if s.markProcessed(chunk.ID()) {
		return nil
	}

	// Skip this chunk if the store already has it
	if hasChunk, err := s.ws.HasChunk(chunk.ID()); err != nil || hasChunk {
		return err
	}

	// The chunk was marked as "processed" above. If there's a problem to actually
	// store it, we need to unmark it again.
	defer func() {
		if err != nil {
			s.unmarkProcessed(chunk.ID())
		}
	}()

	// Store the compressed chunk
	return s.ws.StoreChunk(chunk)
}
