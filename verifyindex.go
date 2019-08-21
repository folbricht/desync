package desync

import (
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/sync/errgroup"
)

// VerifyIndex re-calculates the checksums of a blob comparing it to a given index.
// Fails if the index does not match the blob.
func VerifyIndex(ctx context.Context, name string, idx Index, n int, pb ProgressBar) error {
	in := make(chan IndexChunk)
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
	if stat.Size() != int64(idx.Length()) {
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

				// Position the filehandle to the place where the chunk is meant to come
				// from within the file
				if _, err := f.Seek(int64(c.Start), io.SeekStart); err != nil {
					return err
				}

				// Read the whole (uncompressed) chunk into memory
				b := make([]byte, c.Size)
				if _, err := io.ReadFull(f, b); err != nil {
					return err
				}

				// Calculate this chunks checksum and compare to what it's supposed to be
				// according to the index
				sum := Digest.Sum(b)
				if sum != c.ID {
					return fmt.Errorf("checksum does not match chunk %s", c.ID)
				}
			}
			return nil
		})
	}

	// Feed the workers, stop if there are any errors
loop:
	for _, c := range idx.Chunks {
		select {
		case <-ctx.Done():
			break loop
		case in <- c:
		}
	}
	close(in)

	return g.Wait()
}
