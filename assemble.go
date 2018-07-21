package desync

import (
	"context"
	"crypto/sha512"
	"fmt"
	"os"
	"sync"
	"syscall"
)

// AssembleFile re-assembles a file based on a list of index chunks. It runs n
// goroutines, creating one filehandle for the file "name" per goroutine
// and writes to the file simultaneously. If progress is provided, it'll be
// called when a chunk has been processed.
// If the input file exists and is not empty, the algorithm will first
// confirm if the data matches what is expected and only populate areas that
// differ from the expected content. This can be used to complete partly
// written files.
func AssembleFile(ctx context.Context, name string, idx Index, s Store, seeds []Seed, n int, pb ProgressBar) (*ExtractStats, error) {
	type Job struct {
		segment indexSegment
		source  SeedSegment
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		pErr    error
		in      = make(chan Job)
		isBlank bool
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Setup and start the progressbar if any
	if pb != nil {
		pb.SetTotal(len(idx.Chunks))
		pb.Start()
		defer pb.Finish()
	}

	// Initialize stats to be gathered during extraction
	stats := &ExtractStats{
		BytesTotal:  idx.Length(),
		ChunksTotal: len(idx.Chunks),
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

	// Determine is the target exists and create it if not
	info, err := os.Stat(name)
	switch {
	case os.IsNotExist(err):
		f, err := os.Create(name)
		if err != nil {
			return stats, err
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
		return stats, err
	}

	// Determine the blocksize of the target file which is required for reflinking
	blocksize := blocksizeOfFile(name)

	// Prepend a nullchunk seed to the list of seeds to make sure we read that
	// before any large null sections in other seed files
	ns, err := newNullChunkSeed(name, blocksize, idx.Index.ChunkSizeMax)
	if err != nil {
		return stats, err
	}
	defer ns.close()

	// Start a self-seed which will become usable once chunks are written contigously
	// beginning at position 0.
	ss, err := newSelfSeed(name, idx)
	if err != nil {
		return stats, err
	}
	seeds = append([]Seed{ns, ss}, seeds...)

	// Record the total number of seeds and blocksize in the stats
	stats.Seeds = len(seeds)
	stats.Blocksize = blocksize

	// Start the workers, each having its own filehandle to write concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.OpenFile(name, os.O_RDWR, 0666)
		if err != nil {
			return stats, fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()
		go func() {
			for job := range in {
				if pb != nil {
					pb.Add(job.segment.lengthChunks())
				}
				if job.source != nil {
					stats.addChunksFromSeed(uint64(job.segment.lengthChunks()))
					offset := job.segment.start()
					length := job.segment.lengthBytes()
					copied, cloned, err := job.source.WriteInto(f, offset, length, blocksize, isBlank)
					if err != nil {
						recordError(err)
						continue
					}
					stats.addBytesCopied(copied)
					stats.addBytesCloned(cloned)
					// Record this segment's been written in the self-seed to make it
					// available going forward
					ss.add(job.segment)
					continue
				}
				c := job.segment.chunks()[0]
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
						// Record this chunk's been written in the self-seed
						ss.add(job.segment)
						// Record we kept this chunk in the file (when using in-place extract)
						stats.incChunksInPlace()
						continue
					}
				}
				// Record this chunk having been pulled from the store
				stats.incChunksFromStore()
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
				// Record this chunk's been written in the self-seed
				ss.add(job.segment)
			}
			wg.Done()
		}()
	}

	// Let the sequencer break up the index into segments, feed the workers, and
	// stop if there are any errors
	seq := NewSeedSequencer(idx, seeds...)
loop:
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		chunks, from, done := seq.Next()
		in <- Job{chunks, from}
		if done {
			break
		}
	}
	close(in)

	wg.Wait()
	return stats, pErr
}

func blocksizeOfFile(name string) uint64 {
	stat, err := os.Stat(name)
	if err != nil {
		return DefaultBlockSize
	}
	switch sys := stat.Sys().(type) {
	case *syscall.Stat_t:
		return uint64(sys.Blksize)
	default:
		return DefaultBlockSize
	}
}
