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
				// Update progress bar if any
				if pb != nil {
					pb.Increment()
				}

				// Reuse the fileSeedSegment structure, this is really just a seed segment after all
				segment := newFileSeedSegment(name, c, false, false)
				if err := segment.validate(f); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Feed the workers, stop if there are any errors
loop:
	for i, _ := range idx.Chunks {
		select {
		case <-ctx.Done():
			break loop
		case in <- idx.Chunks[i : i+1]:
		}
	}
	close(in)

	return g.Wait()
}
