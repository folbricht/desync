package desync

import (
	"testing"
	"testing/synctest"
	"time"

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
