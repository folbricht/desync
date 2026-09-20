package desync

import (
	"errors"
	"time"
)

var _ Store = &AdaptiveStore{}
var _ WriteStore = &AdaptiveWriteStore{}

// AdaptiveStore wraps a store and limits the number of concurrent requests
// made to it. The limit adapts to what the network and the store can handle,
// based on the latency of the requests. It starts at 10 and goes up to a
// maximum.
type AdaptiveStore struct {
	s Store
	l *adaptiveLimiter
}

// AdaptiveWriteStore does the same as AdaptiveStore but implements WriteStore
// as well.
type AdaptiveWriteStore struct {
	AdaptiveStore
}

// NewAdaptiveStore returns a store that makes up to max concurrent requests
// to s.
func NewAdaptiveStore(s Store, max int) *AdaptiveStore {
	return &AdaptiveStore{s: s, l: newAdaptiveLimiter(s.String(), max)}
}

// NewAdaptiveWriteStore returns a writable store that makes up to max
// concurrent requests to s.
func NewAdaptiveWriteStore(s WriteStore, max int) *AdaptiveWriteStore {
	return &AdaptiveWriteStore{*NewAdaptiveStore(s, max)}
}

// GetChunk reads and returns one chunk from the store.
func (s *AdaptiveStore) GetChunk(id ChunkID) (*Chunk, error) {
	var chunk *Chunk
	err := s.do(func() (err error) {
		chunk, err = s.s.GetChunk(id)
		return err
	})
	return chunk, err
}

// HasChunk returns true if the chunk is in the store.
func (s *AdaptiveStore) HasChunk(id ChunkID) (bool, error) {
	var hasChunk bool
	err := s.do(func() (err error) {
		hasChunk, err = s.s.HasChunk(id)
		return err
	})
	return hasChunk, err
}

func (s *AdaptiveStore) String() string {
	return s.s.String()
}

// Close closes the wrapped store.
func (s *AdaptiveStore) Close() error {
	return s.s.Close()
}

// StoreChunk adds a new chunk to the store.
func (s *AdaptiveWriteStore) StoreChunk(chunk *Chunk) error {
	return s.do(func() error {
		return s.s.(WriteStore).StoreChunk(chunk)
	})
}

// do makes a request to the wrapped store within the current limit, and
// records how long it took. A missing chunk is an answer like any other, not
// a failure.
func (s *AdaptiveStore) do(request func() error) error {
	s.l.acquire()
	start := time.Now()
	err := request()
	var missing ChunkMissing
	s.l.release(time.Since(start), err != nil && !errors.As(err, &missing))
	return err
}
