package desync

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Copy reads a list of chunks from the provided src store, and copies the ones
// not already present in the dst store. The goal is to load chunks from remote
// store to populate a cache. If progress is provided, it'll be called when a
// chunk has been processed. Used to draw a progress bar, can be nil.
func Copy(ctx context.Context, ids []ChunkID, src Store, dst WriteStore, n int, pb ProgressBar) error {
	in := make(chan ChunkID)
	g, ctx := errgroup.WithContext(ctx)

	// Setup and start the progressbar if any
	pb.SetTotal(len(ids))
	pb.Start()
	defer pb.Finish()

	// Start the workers
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for id := range in {
				pb.Increment()
				hasChunk, err := dst.HasChunk(id)
				if err != nil {
					return err
				}
				if hasChunk {
					continue
				}
				chunk, err := src.GetChunk(id)
				if err != nil {
					return err
				}
				if err := dst.StoreChunk(chunk); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Feed the workers, the context is cancelled if any goroutine encounters an error
loop:
	for _, c := range ids {
		select {
		case <-ctx.Done():
			break loop
		case in <- c:
		}
	}
	close(in)

	return g.Wait()
}
