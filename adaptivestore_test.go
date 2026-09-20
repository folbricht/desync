package desync

import (
	"errors"
	"testing"

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

func TestAdaptiveWriteStore(t *testing.T) {
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
	assert.Equal(t, adaptiveInitialLimit/2, s.l.currentLimit())
}

func TestAdaptiveStoreMissingChunkIsNoFailure(t *testing.T) {
	s := NewAdaptiveStore(failingStore{ChunkMissing{}}, 128)
	_, err := s.GetChunk(ChunkID{})
	require.ErrorAs(t, err, &ChunkMissing{})
	assert.Equal(t, adaptiveInitialLimit, s.l.currentLimit())
}
