package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"sync"
)

// ChopFile split a file according to a list of chunks obtained from an Index
// and stores them in the provided store
func ChopFile(ctx context.Context, name string, chunks []IndexChunk, ws WriteStore, n int, pb ProgressBar) error {
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
		pb.SetTotal(len(chunks))
		pb.Start()
		defer pb.Finish()
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

	s := NewChunkStorage(ws)

	// Start the workers, each having its own filehandle to read concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)

		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()

		go func() {
			defer wg.Done()
			for c := range in {
				// Update progress bar if any
				if pb != nil {
					pb.Add(1)
				}

				var err error
				b, err := readChunkFromFile(f, c)
				if err != nil {
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

				if err := s.StoreChunk(c.ID, b); err != nil {
					recordError(err)
					continue
				}
			}
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

	return pErr
}

// Helper function to read chunk contents from file
func readChunkFromFile(f *os.File, c IndexChunk) ([]byte, error) {
	var err error
	b := make([]byte, c.Size)

	// Position the filehandle to the place where the chunk is meant to come
	// from within the file
	if _, err = f.Seek(int64(c.Start), io.SeekStart); err != nil {
		return b, err
	}
	// Read the whole (uncompressed) chunk into memory

	if _, err = io.ReadFull(f, b); err != nil {
		return b, err
	}

	return b, nil
}
