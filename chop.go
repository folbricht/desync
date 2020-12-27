package desync

import (
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/sync/errgroup"
)

// ChopFile split a file according to a list of chunks obtained from an Index
// and stores them in the provided store
func ChopFile(ctx context.Context, name string, chunks []IndexChunk, ws WriteStore, n int, pb ProgressBar) error {
	in := make(chan IndexChunk)
	g, ctx := errgroup.WithContext(ctx)

	// Setup and start the progressbar if any
	if pb != nil {
		pb.SetTotal(len(chunks))
		pb.Start()
		defer pb.Finish()
	}

	s := NewChunkStorage(ws)

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
					pb.Add(1)
				}

				chunk, err := readChunkFromFile(f, c)
				if err != nil {
					return err
				}

				if err := s.StoreChunk(chunk); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Feed the workers, stop if there are any errors
loop:
	for _, c := range chunks {
		select {
		case <-ctx.Done():
			break loop
		case in <- c:
		}
	}

	close(in)

	return g.Wait()
}

// Helper function to read chunk contents from file
func readChunkFromFile(f *os.File, c IndexChunk) (*Chunk, error) {
	var err error
	b := make([]byte, c.Size)

	// Position the filehandle to the place where the chunk is meant to come
	// from within the file
	if _, err = f.Seek(int64(c.Start), io.SeekStart); err != nil {
		return nil, err
	}
	// Read the whole (uncompressed) chunk into memory
	if _, err = io.ReadFull(f, b); err != nil {
		return nil, err
	}
	return NewChunkWithID(c.ID, b, false)
}
