package desync

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test read access before write access to ensure a failing read doesn't
// impact the write operation (should use separate queues).
func TestWriteDedupQueueParallelReadWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewChunk([]byte{1, 2, 3, 4})
		sleeping := make(chan struct{})
		store := &TestStore{
			// Slow GetChunk operation
			GetChunkFunc: func(id ChunkID) (*Chunk, error) {
				close(sleeping)
				time.Sleep(time.Second)
				return nil, ChunkMissing{id}
			},
		}
		q := NewWriteDedupQueue(store)

		// Queue up a slow GetChunk() operation, then perform a StoreChunk(). The store
		// operation should not be impacted by the ongoing read
		done := make(chan struct{})
		go func() {
			defer close(done)
			q.GetChunk(c.ID())
		}()
		<-sleeping

		start := time.Now()
		require.NoError(t, q.StoreChunk(c))

		// The fake clock only advances while all goroutines are blocked, so any
		// time passing here means the write waited for the in-flight read
		require.Zero(t, time.Since(start), "StoreChunk() blocked on the ongoing GetChunk()")

		// Wait for the read to finish, all goroutines must be done before the test returns
		<-done
	})
}

func TestWriteDedupQueueChunkNotShared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("some data")
		compressed, err := Compress(data)
		require.NoError(t, err)
		id := NewChunk(data).ID()

		// Use a chunk in storage form, its plain data is materialized (and
		// cached) lazily when the readers ask for it
		chunkIn, err := NewChunkFromStorage(id, compressed, Converters{Compressor{}}, true)
		require.NoError(t, err)

		inFlight := make(chan struct{})
		store := &TestStore{
			StoreChunkFunc: func(chunk *Chunk) error {
				close(inFlight)
				// The fake clock only advances once all other goroutines are
				// blocked, so this guarantees the readers registered as
				// waiters on this request before it completes
				time.Sleep(time.Millisecond)
				return nil
			},
		}
		q := NewWriteDedupQueue(store)

		// Store a chunk while several readers request the same chunk ID. Every
		// reader must get its own copy since accessing the plain data modifies
		// the chunk, and the writer keeps using the original. Run with -race
		// to confirm the callers don't share state.
		var wg sync.WaitGroup
		chunks := make(chan *Chunk, 10)
		wg.Go(func() {
			assert.NoError(t, q.StoreChunk(chunkIn))
		})
		<-inFlight
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

		seen := map[*Chunk]struct{}{chunkIn: {}}
		for c := range chunks {
			_, shared := seen[c]
			assert.False(t, shared, "the same *Chunk was handed to multiple callers")
			seen[c] = struct{}{}
		}
	})
}
