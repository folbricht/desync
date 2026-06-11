package desync

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	synctest.Test(t, func(t *testing.T) {
		// Make a store that counts the requests to it
		var requests atomic.Int64
		store := &TestStore{
			GetChunkFunc: func(ChunkID) (*Chunk, error) {
				// The fake clock only advances once all other goroutines are
				// blocked, so this guarantees they all registered as waiters
				// on this request before it completes
				time.Sleep(time.Millisecond)
				requests.Add(1)
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
			wg.Go(func() {
				<-start
				q.GetChunk(ChunkID{0})
			})
		}

		close(start)
		wg.Wait()

		// There should be just one request that was done on the upstream store
		require.EqualValues(t, 1, requests.Load(), "requests to the store")
	})
}
