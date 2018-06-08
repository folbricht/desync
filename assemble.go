package desync

import (
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"os"
	"sync"
)

// AssembleFile re-assembles a file based on a list of index chunks. It runs n
// goroutines, creating one filehandle for the file "name" per goroutine
// and writes to the file simultaneously. If progress is provided, it'll be
// called when a chunk has been processed.
// If the input file exists and is not empty, the algorithm will first
// confirm if the data matches what is expected and only populate areas that
// differ from the expected content. This can be used to complete partly
// written files.
func AssembleFile(ctx context.Context, name string, idx Index, s Store, n int, progress func()) error {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		pErr      error
		in        = make(chan IndexChunk)
		nullChunk = NewNullChunk(idx.Index.ChunkSizeMax)
		isBlank   bool
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

	// Determine is the target exists and create it if not
	info, err := os.Stat(name)
	switch {
	case os.IsNotExist(err):
		f, err := os.Create(name)
		if err != nil {
			return err
		}
		f.Close()
		isBlank = true
	case info.Size() == 0:
		isBlank = true
	}

	// Truncate the output file to the full expected size. Not only does this
	// confirm there's enough disk space, but it allows for an optimization
	// when dealing with the Null Chunk
	if err := os.Truncate(name, idx.Length()); err != nil {
		return err
	}

	// Keep a record of what's already been written to the file and can be
	// re-used if there are duplicate chunks
	var written fileChunks

	// Start the workers, each having its own filehandle to write concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.OpenFile(name, os.O_RDWR, 0666)
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
				// null chunk is being requested. If a new file is truncated to the
				// right size beforehand, there's nothing to do since everything
				// defaults to 0 bytes.
				if isBlank && c.ID == nullChunk.ID {
					continue
				}
				// If we operate on an existing file there's a good chance we already
				// have the data written for this chunk. Let's read it from disk and
				// compare to what is expected.
				if !isBlank {
					b := make([]byte, c.Size)
					if _, err := f.ReadAt(b, int64(c.Start)); err != nil {
						recordError(err)
						continue
					}
					sum := sha512.Sum512_256(b)
					if sum == c.ID {
						written.add(c)
						continue
					}
				}
				// Before pulling a chunk from the store, let's see if that same chunk's
				// been written to the file already. If so, we can simply clone it from
				// that location.
				if cw, ok := written.get(c.ID); ok {
					if err := cloneInFile(f, c, cw); err != nil {
						recordError(err)
					}
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
				// Make a record of this chunk being available in the file now
				written.add(c)
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

// fileChunks acts as a kind of in-file cache for chunks already written to
// the file being assembled. Every chunk ref that has been successfully written
// into the file is added to it. If another write operation requires the same
// (duplicate) chunk again, it can just copied out of the file to the new
// position, rather than requesting it from a (possibly remote) store again
// and decompressing it.
type fileChunks struct {
	mu     sync.RWMutex
	chunks map[ChunkID]IndexChunk
}

func (f *fileChunks) add(c IndexChunk) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.chunks) == 0 {
		f.chunks = make(map[ChunkID]IndexChunk)
	}
	f.chunks[c.ID] = c
}

func (f *fileChunks) get(id ChunkID) (IndexChunk, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, ok := f.chunks[id]
	return c, ok
}

// cloneInFile copies a chunk from one position to another in the same file.
// Used when duplicate chunks are used in a file. TODO: The current implementation
// uses just the one given filehandle, copies into memory, then writes to disk.
// It may be more efficient to open a 2nd filehandle, seek, and copy directly
// with a io.LimitReader.
func cloneInFile(f *os.File, dst, src IndexChunk) error {
	if src.ID != dst.ID || src.Size != dst.Size {
		return errors.New("internal error: different chunks requested for in-file copy")
	}
	b := make([]byte, int64(src.Size))
	if _, err := f.ReadAt(b, int64(src.Start)); err != nil {
		return err
	}
	_, err := f.WriteAt(b, int64(dst.Start))
	return err
}
