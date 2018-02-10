package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"os"
	"sync"
)

// AssembleFile re-assembles a file based on a list of index chunks. It runs n
// goroutines, creating one filehandle for the file "name" per goroutine
// and writes to the file simultaneously. If progress is provided, it'll be
// called when a chunk has been processed.
func AssembleFile(ctx context.Context, name string, idx Index, s Store, n int, progress func()) error {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		pErr      error
		in        = make(chan IndexChunk)
		nullChunk = NewNullChunk(idx.Index.ChunkSizeMax)
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

	// Truncate the output file to the full expected size. Not only does this
	// confirm there's enough disk space, but it allow allows for an optimization
	// when dealing with the Null Chunk
	if err := os.Truncate(name, idx.Length()); err != nil {
		return err
	}

	// Start the workers, each having its own filehandle to write concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()
		go func() {
			for c := range in {
				if progress != nil {
					progress()
				}
				// See if we can skip the chunk retrieval and decompression if the
				// null chunk is being requested. If the file is truncated to the
				// right size beforehand, there's nothing to do since everything
				// defaults to 0 bytes.
				if c.ID == nullChunk.ID {
					continue
				}
				// Pull the (compressed) chunk from the store
				b, err := s.GetChunk(c.ID)
				if err != nil {
					recordError(err)
					continue
				}
				// Since we know how big the chunk is supposed to be, pre-allocate a
				// slice to decompress into
				var db []byte
				db = make([]byte, c.Size)
				// The the chunk is compressed. Decompress it here
				db, err = Decompress(db, b)
				if err != nil {
					recordError(err)
					continue
				}
				// Verify the checksum of the chunk matches the ID
				sum := sha512.Sum512_256(db)
				if sum != c.ID {
					recordError(fmt.Errorf("unexpected sha512/256 %s for chunk id %s", sum, c.ID))
					continue
				}
				// Might as well verify the chunk size while we're at it
				if c.Size != uint64(len(db)) {
					recordError(fmt.Errorf("unexpected size for chunk %s", c.ID))
					continue
				}
				// Write the decompressed chunk into the file at the right position
				if _, err = f.WriteAt(db, int64(c.Start)); err != nil {
					recordError(err)
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
