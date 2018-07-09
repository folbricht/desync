package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"sync"
)

// VerifyIndex re-calculates the checksums of a blob comparing it to a given index.
// Fails if the index does not match the blob.
func VerifyIndex(ctx context.Context, name string, idx Index, n int, pb ProgressBar) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		pErr error
		in   = make(chan IndexChunk)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if pErr == nil {
			pErr = err
		}
		cancel()
	}

	// Start the workers, each having its own filehandle to read concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()
		go func() {
			var err error
			for c := range in {
				// Update progress bar if any
				if pb != nil {
					pb.Increment()
				}

				// Position the filehandle to the place where the chunk is meant to come
				// from within the file
				if _, err = f.Seek(int64(c.Start), io.SeekStart); err != nil {
					recordError(err)
					continue
				}

				// Read the whole (uncompressed) chunk into memory
				b := make([]byte, c.Size)
				if _, err = io.ReadFull(f, b); err != nil {
					recordError(err)
					continue
				}

				// Calculate this chunks checksum and compare to what it's supposed to be
				// according to the index
				sum := sha512.Sum512_256(b)
				if sum != c.ID {
					recordError(fmt.Errorf("checksum does not match chunk %s", c.ID))
					continue
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop if there are any errors
loop:
	for _, c := range idx.Chunks {
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
