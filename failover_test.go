package desync

import (
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestFailoverMissingChunk(t *testing.T) {
	s := &TestStore{}
	g := NewFailoverGroup(s)
	_, err := g.GetChunk(ChunkID{0})
	if _, ok := err.(ChunkMissing); !ok {
		t.Fatalf("expected missing chunk error, got %T", err)
	}
}

func TestFailoverAllError(t *testing.T) {
	var failed = errors.New("failed")
	storeFail := &TestStore{
		GetChunkFunc: func(ChunkID) (*Chunk, error) { return nil, failed },
	}
	g := NewFailoverGroup(storeFail, storeFail)
	if _, err := g.GetChunk(ChunkID{0}); err != failed {
		t.Fatalf("expected error, got %T", err)
	}
}

func TestFailoverSimple(t *testing.T) {
	// Create two stores, one that always fails and one that works
	storeFail := &TestStore{
		GetChunkFunc: func(ChunkID) (*Chunk, error) { return nil, errors.New("failed") },
	}
	storeSucc := &TestStore{
		GetChunkFunc: func(ChunkID) (*Chunk, error) { return nil, nil },
	}

	// Group the two stores together, the failing ones first
	g := NewFailoverGroup(storeFail, storeFail, storeSucc)

	// Request a chunk, should succeed
	if _, err := g.GetChunk(ChunkID{0}); err != nil {
		t.Fatal(err)
	}

	// Look inside the group to confirm we failed over to the last one
	if g.active != 2 {
		t.Fatalf("expected g.active=1, but got %d", g.active)
	}
}

func TestFailoverMutliple(t *testing.T) {
	// Create two stores, one that fails when x is 1 and the other fails when x is 0
	var x int64
	storeA := &TestStore{
		GetChunkFunc: func(id ChunkID) (*Chunk, error) {
			if atomic.LoadInt64(&x) == 0 {
				return nil, nil
			}
			return nil, errors.New("failed")
		},
	}
	storeB := &TestStore{
		GetChunkFunc: func(id ChunkID) (*Chunk, error) {
			if atomic.LoadInt64(&x) == 1 {
				return nil, nil
			}
			return nil, errors.New("failed")
		},
	}

	// Group the two stores together, the failing ones first
	g := NewFailoverGroup(storeA, storeB)

	var (
		wg       sync.WaitGroup
		done     = make(chan struct{})
		timeout  = time.After(time.Second)
		failOver = time.Tick(10 * time.Millisecond)
	)

	// Run several goroutines querying the group in a tight loop
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			var id ChunkID
			for {
				time.Sleep(time.Millisecond)
				select {
				case <-done:
					wg.Done()
					return
				default:
					rand.Read(id[:])
					if _, err := g.GetChunk(id); err != nil {
						t.Fatal(err)
					}
				}
			}
		}()
	}

	// Make the stores fail over every 10 ms
	go func() {
		wg.Add(1)
		for {
			select {
			case <-timeout: // done running
				close(done)
				wg.Done()
				return
			case <-failOver: // switch over to the other store
				newX := (x + 1) % 2
				atomic.StoreInt64(&x, newX)
			}
		}
	}()

	wg.Wait()
}
