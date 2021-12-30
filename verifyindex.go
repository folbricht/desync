package desync

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"
)

// VerifyIndex re-calculates the checksums of a blob comparing it to a given index.
// Fails if the index does not match the blob.
func VerifyIndex(ctx context.Context, name string, idx Index, n int, pb ProgressBar) error {
	in := make(chan []IndexChunk)
	g, ctx := errgroup.WithContext(ctx)

	// Setup and start the progressbar if any
	if pb != nil {
		pb.SetTotal(len(idx.Chunks))
		pb.Start()
		defer pb.Finish()
	}

	stat, err := os.Stat(name)
	if err != nil {
		return err
	}
	if !isDevice(stat.Mode()) && stat.Size() != int64(idx.Length()) {
		return fmt.Errorf("index size (%d) does not match file size (%d)", idx.Length(), stat.Size())
	}

	// Start the workers, each having its own filehandle to read concurrently
	for i := 0; i < n; i++ {
		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()
		g.Go(func() error {
			for c := range in {
				// Reuse the fileSeedSegment structure, this is really just a seed segment after all
				segment := newFileSeedSegment(name, c, false)
				if err := segment.Validate(f); err != nil {
					return err
				}

				// Update progress bar, if any
				if pb != nil {
					pb.Add(len(c))
				}
			}
			return nil
		})
	}

	chunksNum := len(idx.Chunks)

	// Number of chunks that will be evaluated in a single Goroutine.
	// This helps to reduce the required number of context switch.
	// In theory, we could just divide the total number of chunks by the number
	// of workers, but instead we reduce that by 10 times to avoid the situation
	// where we end up waiting a single worker that was slower to complete (e.g.
	// if its chunks were not in cache while the others were).
	batch := chunksNum / (n * 10)

	// Feed the workers, stop if there are any errors
loop:
	for i := 0; i < chunksNum; i = i + batch + 1 {
		last := i + batch
		if last >= chunksNum {
			// We reached the end of the array
			last = chunksNum - 1
		}
		select {
		case <-ctx.Done():
			break loop
		case in <- idx.Chunks[i : last+1]:
		}
	}
	close(in)

	return g.Wait()
}
