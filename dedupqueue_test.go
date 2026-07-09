package desync

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestDedupQueueChunkNotShared(t *testing.T) {
	data := []byte("some data")
	id := NewChunk(data).ID()
	compressed, err := Compress(data)
	require.NoError(t, err)

	store := &TestStore{
		GetChunkFunc: func(id ChunkID) (*Chunk, error) {
			// Give the other goroutines time to pile up on this request
			time.Sleep(10 * time.Millisecond)
			// Return the chunk in storage form, the plain data is only
			// materialized lazily when the caller asks for it
			return NewChunkFromStorage(id, compressed, Converters{Compressor{}}, true)
		},
	}
	q := NewDedupQueue(store)

	// Request the same chunk from many goroutines at once. Each caller must
	// receive its own copy since accessing the plain data modifies the chunk.
	// Run with -race to confirm the callers don't share state.
	chunks := make(chan *Chunk, 10)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			c, err := q.GetChunk(id)
			if !assert.NoError(t, err) {
				return
			}
			b, err := c.Data()
			assert.NoError(t, err)
			assert.Equal(t, data, b)
			chunks <- c
		})
	}
	wg.Wait()
	close(chunks)

	seen := make(map[*Chunk]struct{})
	for c := range chunks {
		_, shared := seen[c]
		assert.False(t, shared, "the same *Chunk was handed to multiple callers")
		seen[c] = struct{}{}
	}
}
