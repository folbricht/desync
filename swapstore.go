package desync

import (
	"sync"

	"github.com/pkg/errors"
)

var _ Store = &SwapStore{}
var _ WriteStore = &SwapWriteStore{}

// SwapStore wraps another store and provides the ability to swap out the underlying
// store with another one while under load. Typically used to reload config for
// long-running processes, perhaps reloading a store config file on SIGHUP and
// updating the store on-the-fly without restart.
type SwapStore struct {
	s Store

	mu sync.RWMutex
}

// SwapWriteStore does ther same as SwapStore but implements WriteStore as well.
type SwapWriteStore struct {
	SwapStore
}

// NewSwapStore creates an instance of a swap store wrappert that allows replacing
// the wrapped store at runtime.
func NewSwapStore(s Store) *SwapStore {
	return &SwapStore{s: s}
}

// NewSwapWriteStore initializes as new instance of a swap store that supports
// writing and swapping at runtime.
func NewSwapWriteStore(s Store) *SwapWriteStore {
	return &SwapWriteStore{SwapStore{s: s}}
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s *SwapStore) GetChunk(id ChunkID) (*Chunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s.GetChunk(id)
}

// HasChunk returns true if the chunk is in the store
func (s *SwapStore) HasChunk(id ChunkID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s.HasChunk(id)
}

func (s *SwapStore) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s.String()
}

// Close the store. NOP opertation, needed to implement Store interface.
func (s *SwapStore) Close() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s.Close()
}

// Close the store. NOP opertation, needed to implement Store interface.
func (s *SwapStore) Swap(new Store) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, oldWritable := s.s.(WriteStore)
	_, newWritable := new.(WriteStore)
	if oldWritable && !newWritable {
		return errors.New("a writable store can obly be updated with another writable one")
	}
	s.s.Close() // Close the old store
	s.s = new
	return nil
}

// StoreChunk adds a new chunk to the store
func (s *SwapWriteStore) StoreChunk(chunk *Chunk) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s.(WriteStore).StoreChunk(chunk)
}
