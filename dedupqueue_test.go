package desync

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDedupQueueSimple(t *testing.T) {
	// var requests int64
	// store := &TestStore{
	// 	GetChunkFunc: func(ChunkID) (*Chunk, error) {
	// 		atomic.AddInt64(&requests, 1)
	// 		return NewChunkFromUncompressed([]byte{0}), nil
	// 	},
	// }
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
	if err != nil {
		t.Fatal(err)
	}
	bActual, err := q.GetChunk(exists)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bActual, bExpected) {
		t.Fatalf("got %v; want %v", bExpected, bActual)
	}

	// Now make sure errors too are passed correctly
	_, err = q.GetChunk(notExists)
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatalf("got '%v'; want chunk missing error", err)
	}

	// Check HasChunk() as well
	hasChunk, err := q.HasChunk(exists)
	if err != nil {
		t.Fatal(err)
	}
	if !hasChunk {
		t.Fatalf("HasChunk() = false; want true")
	}
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
	for i := 0; i < 10; i++ {
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
	if requests > 1 {
		t.Fatalf("%d requests to the store; want 1", requests)
	}
}
