package desync

import (
	"errors"
	"time"
)

var _ Store = &LimitedStore{}
var _ WriteStore = &LimitedWriteStore{}

// concurrencyLimiter decides how many requests can be made to a store at the
// same time.
type concurrencyLimiter interface {
	// acquire blocks until a request can be made.
	acquire()
	// release records a completed request, how long it took, and whether it
	// failed.
	release(latency time.Duration, failed bool)
}

// fixedLimiter allows a fixed number of concurrent requests.
type fixedLimiter chan struct{}

func (l fixedLimiter) acquire()                    { l <- struct{}{} }
func (l fixedLimiter) release(time.Duration, bool) { <-l }

// LimitedStore wraps a store and limits the number of concurrent requests
// made to it, either to a fixed number or to one that adapts to what the
// network and the store can handle.
type LimitedStore struct {
	s Store
	l concurrencyLimiter
}

// LimitedWriteStore does the same as LimitedStore but implements WriteStore
// as well.
type LimitedWriteStore struct {
	LimitedStore
}

// NewLimitedStore returns a store that makes up to n concurrent requests to s.
func NewLimitedStore(s Store, n int) *LimitedStore {
	return &LimitedStore{s: s, l: make(fixedLimiter, max(n, 1))}
}

// NewLimitedWriteStore returns a writable store that makes up to n concurrent
// requests to s.
func NewLimitedWriteStore(s WriteStore, n int) *LimitedWriteStore {
	return &LimitedWriteStore{*NewLimitedStore(s, n)}
}

// NewAdaptiveStore returns a store that limits the number of concurrent
// requests to s based on their throughput and latency. The limit starts at 10
// and goes up to max.
func NewAdaptiveStore(s Store, max int) *LimitedStore {
	return &LimitedStore{s: s, l: newAdaptiveLimiter(s.String(), max)}
}

// NewAdaptiveWriteStore returns a writable store that limits the number of
// concurrent requests to s like NewAdaptiveStore does.
func NewAdaptiveWriteStore(s WriteStore, max int) *LimitedWriteStore {
	return &LimitedWriteStore{*NewAdaptiveStore(s, max)}
}

// GetChunk reads and returns one chunk from the store.
func (s *LimitedStore) GetChunk(id ChunkID) (*Chunk, error) {
	var chunk *Chunk
	err := s.do(func() (err error) {
		chunk, err = s.s.GetChunk(id)
		return err
	})
	return chunk, err
}

// HasChunk returns true if the chunk is in the store.
func (s *LimitedStore) HasChunk(id ChunkID) (bool, error) {
	var hasChunk bool
	err := s.do(func() (err error) {
		hasChunk, err = s.s.HasChunk(id)
		return err
	})
	return hasChunk, err
}

func (s *LimitedStore) String() string {
	return s.s.String()
}

// Close closes the wrapped store.
func (s *LimitedStore) Close() error {
	return s.s.Close()
}

// StoreChunk adds a new chunk to the store.
func (s *LimitedWriteStore) StoreChunk(chunk *Chunk) error {
	return s.do(func() error {
		return s.s.(WriteStore).StoreChunk(chunk)
	})
}

// do makes a request to the wrapped store within the current limit, and
// records how long it took. A missing chunk is an answer like any other, not
// a failure.
func (s *LimitedStore) do(request func() error) error {
	s.l.acquire()
	start := time.Now()
	err := request()
	var missing ChunkMissing
	s.l.release(time.Since(start), err != nil && !errors.As(err, &missing))
	return err
}
