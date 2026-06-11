package desync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDedupQueueSimple(t *testing.T) {
	exists := ChunkID{0}
	notExists := ChunkID{1}
	store := &TestStore{
		Chunks: map[ChunkID][]byte{
			exists: {0, 1, 2, 3},
		},
	}
	q := NewDedupQueue(store)

	// First compare we're getting the expected data in the positive case
	bExpected, err := store.GetChunk(exists)
	require.NoError(t, err)
	bActual, err := q.GetChunk(exists)
	require.NoError(t, err)
	require.Equal(t, bExpected, bActual)

	// Now make sure errors too are passed correctly
	_, err = q.GetChunk(notExists)
	require.IsType(t, ChunkMissing{}, err)

	// Check HasChunk() as well
	hasChunk, err := q.HasChunk(exists)
	require.NoError(t, err)
	require.True(t, hasChunk)
}

func TestDedupQueueParallel(t *testing.T) {
	// Make a store that counts the requests to it
	var requests int64
	store := &TestStore{
		GetChunkFunc: func(ChunkID) (*Chunk, error) {
			time.Sleep(time.Millisecond) // make it artificially slow to not complete too early
			atomic.AddInt64(&requests, 1)
			return NewChunk([]byte{0}), nil
		},
	}
	q := NewDedupQueue(store)

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	// Start several goroutines all asking for the same chunk from the store
	for range 10 {
		wg.Add(1)
		go func() {
			<-start
			q.GetChunk(ChunkID{0})
			wg.Done()
		}()
	}

	close(start)
	wg.Wait()

	// There should ideally be just one requests that was done on the upstream store
	require.LessOrEqual(t, requests, int64(1), "requests to the store")
}
