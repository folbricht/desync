package desync

import (
	"strings"
	"sync"
)

var _ Store = &FailoverGroup{}

// FailoverGroup wraps multiple stores to provide failover when one or more stores in the group fail.
// Only one of the stores in the group is considered "active" at a time. If an unexpected error is returned
// from the active store, the next store in the group becomes the active one and the request retried.
// When all stores returned a failure, the group will pass up the failure to the caller. The active store
// rotates through all available stores. All stores in the group are expected to contain the same chunks,
// there is no failover for missing chunks. Implements the Store interface.
type FailoverGroup struct {
	stores []Store
	active int
	mu     sync.RWMutex
}

// NewFailoverGroup initializes and returns a store wraps multiple stores to form a group that can fail over
// between them on failure from one.
func NewFailoverGroup(stores ...Store) *FailoverGroup {
	return &FailoverGroup{stores: stores}
}

func (g *FailoverGroup) GetChunk(id ChunkID) (*Chunk, error) {
	var gErr error
	for i := 0; i < len(g.stores); i++ {
		s, active := g.current()
		b, err := s.GetChunk(id)
		if err == nil { // return right away on success
			return b, err
		}

		// All stores are meant to hold the same chunks, fail on the first missing chunk
		if _, ok := err.(ChunkMissing); ok {
			return b, err
		}

		// Record the error to be returned when all requests fail
		gErr = err

		// Fail over to the next store
		g.errorFrom(active)
	}
	return nil, gErr
}

func (g *FailoverGroup) HasChunk(id ChunkID) (bool, error) {
	var gErr error
	for i := 0; i < len(g.stores); i++ {
		s, active := g.current()
		hc, err := s.HasChunk(id)
		if err == nil { // return right away on success
			return hc, err
		}

		// Record the error to be returned when all requests fail
		gErr = err

		// Fail over to the next store
		g.errorFrom(active)
	}
	return false, gErr
}

func (g *FailoverGroup) String() string {
	var str []string
	for _, s := range g.stores {
		str = append(str, s.String())
	}
	return strings.Join(str, "|")
}

func (g *FailoverGroup) Close() error {
	var closeErr error
	for _, s := range g.stores {
		if err := s.Close(); err != nil {
			closeErr = err
		}
	}
	return closeErr
}

// Thread-safe method to return the currently active store.
func (g *FailoverGroup) current() (Store, int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.stores[g.active], g.active
}

// Fail over to the next available store after recveiving an error from i (the active). We
// need i to know which store returned the error as there could be failures from concurrent
// requests. Another request could have initiated the failover already. So ignore if i is not
// (no longer) the active store.
func (g *FailoverGroup) errorFrom(i int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if i != g.active {
		return
	}
	g.active = (g.active + 1) % len(g.stores)
}
