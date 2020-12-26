package desync

import (
	"testing"
	"time"
)

// Test read access before write access to ensure a failing read doesn't
// impact the write operation (should use separate queues).
func TestWriteDedupQueueParallelReadWrite(t *testing.T) {
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

	// Queue us a slow HasChunk() operation, then perform a StoreChunk(). The store
	// operation should not be impacted by the ongoing read
	go q.GetChunk(c.ID())
	<-sleeping

	if err := q.StoreChunk(c); err != nil {
		t.Fatal(err)
	}
}
