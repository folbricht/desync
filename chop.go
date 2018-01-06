package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"sync"
)

// ChopFile split a file according to a list of chunks obtained from an Index.
func ChopFile(ctx context.Context, name string, chunks []IndexChunk, s LocalStore, n int) []error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
		in   = make(chan IndexChunk)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
		cancel()
	}

	// Start the workers, each having its own filehandle to read concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.Open(name)
		if err != nil {
			return []error{fmt.Errorf("unable to open file %s, %s", name, err)}
		}
		defer f.Close()
		go func() {
			for c := range in {
				// Skip this chunk if the store already has it
				if s.HasChunk(c.ID) {
					continue
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
					recordError(fmt.Errorf("chunk %s checksum does not match", c.ID))
					continue
				}

				// Compress the chunk
				cb, err := Compress(b)
				if err != nil {
					recordError(err)
					continue
				}

				// And store it
				if err = s.StoreChunk(c.ID, cb); err != nil {
					recordError(err)
					continue
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop if there are any errors
loop:
	for _, c := range chunks {
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

	return errs
}
