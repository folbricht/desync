package desync

import (
	"context"
	"sync"
)

// Touch reads a list of chunks from the provided store, but doesn't actually
// do anything with them. The goal is to load chunks from remote stores and
// cache them. Without a cache being defined in s, this is a NOP. If progress
// is provided, it'll be called when a chunk has been processed. Used to draw
// a progress bar. progress can be nil.
func Touch(ctx context.Context, ids []ChunkID, s Store, n int, progress func()) error {
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
				if _, err := s.GetChunk(id); err != nil {
					recordError(err)
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
