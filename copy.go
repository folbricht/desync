package desync

import (
	"context"
	"sync"
)

// Copy reads a list of chunks from the provided src store, and copies the ones
// not already present in the dst store. The goal is to load chunks from remote
// store to populate a cache. If progress is provided, it'll be called when a
// chunk has been processed. Used to draw a progress bar, can be nil.
func Copy(ctx context.Context, ids []ChunkID, src Store, dst WriteStore, n int, progress func()) error {
	var (
		wg   sync.WaitGroup
		in   = make(chan ChunkID)
		mu   sync.Mutex
		pErr error
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if pErr == nil {
			pErr = err
		}
		cancel()
	}

	// Start the workers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for id := range in {
				if !dst.HasChunk(id) {
					b, err := src.GetChunk(id)
					if err != nil {
						recordError(err)
						continue
					}
					if err := dst.StoreChunk(id, b); err != nil {
						recordError(err)
					}
				}
				if progress != nil {
					progress()
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop on any errors
loop:
	for _, c := range ids {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		in <- c
	}
	close(in)
	wg.Wait()

	return pErr
}
