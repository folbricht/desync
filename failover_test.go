package desync

import (
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestFailoverMissingChunk(t *testing.T) {
	s := &TestStore{}
	g := NewFailoverGroup(s)
	_, err := g.GetChunk(ChunkID{0})
	require.IsType(t, ChunkMissing{}, err)
}

func TestFailoverAllError(t *testing.T) {
	var failed = errors.New("failed")
	storeFail := &TestStore{
		GetChunkFunc: func(ChunkID) (*Chunk, error) { return nil, failed },
	}
	g := NewFailoverGroup(storeFail, storeFail)
	_, err := g.GetChunk(ChunkID{0})
	require.ErrorIs(t, err, failed)
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
	_, err := g.GetChunk(ChunkID{0})
	require.NoError(t, err)

	// Look inside the group to confirm we failed over to the last one
	require.Equal(t, 2, g.active)
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
		ctx, cancel = context.WithTimeout(t.Context(), time.Second)
		eg, gCtx    = errgroup.WithContext(ctx)
		failOver    = time.Tick(10 * time.Millisecond)
	)
	defer cancel()

	// Run several goroutines querying the group in a tight loop
	for range 16 {
		eg.Go(func() error {
			var id ChunkID
			for {
				time.Sleep(time.Millisecond)
				select {
				case <-gCtx.Done():
					return nil
				default:
					rand.Read(id[:])
					if _, err := g.GetChunk(id); err != nil {
						return err
					}
				}
			}
		})
	}

	// Make the stores fail over every 10 ms
	eg.Go(func() error {
		for {
			select {
			case <-gCtx.Done(): // done running
				return nil
			case <-failOver: // switch over to the other store
				newX := (x + 1) % 2
				atomic.StoreInt64(&x, newX)
			}
		}
	})

	require.NoError(t, eg.Wait())
}
