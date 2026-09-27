package desync

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingStore answers every request with the same error.
type failingStore struct {
	err error
}

func (s failingStore) GetChunk(ChunkID) (*Chunk, error) { return nil, s.err }
func (s failingStore) HasChunk(ChunkID) (bool, error)   { return false, s.err }
func (s failingStore) Close() error                     { return nil }
func (s failingStore) String() string                   { return "failing" }

func TestLimitedWriteStore(t *testing.T) {
	local, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)
	s := NewAdaptiveWriteStore(local, 128)
	defer s.Close()

	chunk := NewChunk([]byte("chunk"))
	require.NoError(t, s.StoreChunk(chunk))

	hasChunk, err := s.HasChunk(chunk.ID())
	require.NoError(t, err)
	assert.True(t, hasChunk)

	got, err := s.GetChunk(chunk.ID())
	require.NoError(t, err)
	b, err := got.Data()
	require.NoError(t, err)
	assert.Equal(t, []byte("chunk"), b)
	assert.Equal(t, local.String(), s.String())
}

func TestAdaptiveStoreFailuresBackOff(t *testing.T) {
	s := NewAdaptiveStore(failingStore{errors.New("unavailable")}, 128)
	_, err := s.GetChunk(ChunkID{})
	require.Error(t, err)
	assert.Equal(t, adaptiveInitialLimit/2, s.l.(*adaptiveLimiter).currentLimit())
}

func TestAdaptiveStoreMissingChunkIsNoFailure(t *testing.T) {
	s := NewAdaptiveStore(failingStore{ChunkMissing{}}, 128)
	_, err := s.GetChunk(ChunkID{})
	require.ErrorAs(t, err, &ChunkMissing{})
	assert.Equal(t, adaptiveInitialLimit, s.l.(*adaptiveLimiter).currentLimit())
}

// blockingStore holds every request until it's released.
type blockingStore struct {
	started chan struct{}
	release chan struct{}
}

func (s blockingStore) GetChunk(ChunkID) (*Chunk, error) {
	s.started <- struct{}{}
	<-s.release
	return nil, ChunkMissing{}
}
func (s blockingStore) HasChunk(ChunkID) (bool, error) { return false, nil }
func (s blockingStore) Close() error                   { return nil }
func (s blockingStore) String() string                 { return "blocking" }

func TestLimitedStoreFixedLimit(t *testing.T) {
	store := blockingStore{started: make(chan struct{}), release: make(chan struct{})}
	s := NewLimitedStore(store, 2)
	for range 3 {
		go func() { _, _ = s.GetChunk(ChunkID{}) }()
	}

	for range 2 {
		select {
		case <-store.started:
		case <-time.After(time.Minute):
			require.FailNow(t, "request within the limit wasn't made")
		}
	}
	select {
	case <-store.started:
		require.FailNow(t, "request beyond the limit was made")
	case <-time.After(50 * time.Millisecond):
	}

	// Completing one request lets the next one through
	store.release <- struct{}{}
	select {
	case <-store.started:
	case <-time.After(time.Minute):
		require.FailNow(t, "request wasn't made after another completed")
	}
	store.release <- struct{}{}
	store.release <- struct{}{}
}
