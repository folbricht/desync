package desync

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

type TimeThrottle struct {
	lastExecutionTime               time.Time
	minimumTimeBetweenEachExecution time.Duration
}

func (timeThrottle *TimeThrottle) reset() {
	timeThrottle.lastExecutionTime = time.Now()
}

func (timeThrottle *TimeThrottle) calculateThrottle() (bool, time.Duration) {
	r := -(time.Since(timeThrottle.lastExecutionTime) - timeThrottle.minimumTimeBetweenEachExecution)
	return r > 0, r
}

func (timeThrottle *TimeThrottle) waitIfRequired() {

	wait, duration := timeThrottle.calculateThrottle()
	if wait {
		time.Sleep(duration)
	}
}

func buildThrottle(waitPeriodMillis int) TimeThrottle {

	d := time.Millisecond * time.Duration(waitPeriodMillis)
	return TimeThrottle{time.Now().Add(-d), time.Duration(d)}
}

// Copy reads a list of chunks from the provided src store, and copies the ones
// not already present in the dst store. The goal is to load chunks from remote
// store to populate a cache. If progress is provided, it'll be called when a
// chunk has been processed. Used to draw a progress bar, can be nil.
func Copy(ctx context.Context, ids []ChunkID, src Store, dst WriteStore, n int, pb ProgressBar, shouldThrottle bool, waitPeriodMillis int) error {

	in := make(chan ChunkID)
	g, ctx := errgroup.WithContext(ctx)


	// Setup and start the progressbar if any
	pb.SetTotal(len(ids))
	pb.Start()
	defer pb.Finish()

	// Start the workers
	for i := 0; i < n; i++ {
		g.Go(func() error {
			waitPeriodMillis := 200
			throttle := buildThrottle(waitPeriodMillis)

			for id := range in {
				pb.Increment()
				hasChunk, err := dst.HasChunk(id)
				if err != nil {
					return err
				}
				if hasChunk {
					continue
				}
				if shouldThrottle {
					throttle.waitIfRequired()
				}

				chunk, err := src.GetChunk(id)
				throttle.reset()
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
