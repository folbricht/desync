package desync

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"os"
)

// InvalidSeedAction represent the action that we will take if a seed
// happens to be invalid. There are currently three options:
// - fail with an error
// - skip the invalid seed and try to continue
// - regenerate the invalid seed index
type InvalidSeedAction int

const (
	InvalidSeedActionBailOut InvalidSeedAction = iota
	InvalidSeedActionSkip
	InvalidSeedActionRegenerate
)

type AssembleOptions struct {
	N                 int
	InvalidSeedAction InvalidSeedAction
}

// AssembleFile re-assembles a file based on a list of index chunks. It runs n
// goroutines, creating one filehandle for the file "name" per goroutine
// and writes to the file simultaneously. If progress is provided, it'll be
// called when a chunk has been processed.
// If the input file exists and is not empty, the algorithm will first
// confirm if the data matches what is expected and only populate areas that
// differ from the expected content. This can be used to complete partly
// written files.
func AssembleFile(ctx context.Context, name string, idx Index, s Store, seeds []Seed, options AssembleOptions, pb ProgressBar) (*ExtractStats, error) {
	type Job struct {
		segment IndexSegment
		source  SeedSegment
	}
	var (
		in          = make(chan Job)
		isBlank     bool
		isBlkDevice bool
	)
	g, ctx := errgroup.WithContext(ctx)

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

	// Determine is the target exists and create it if not
	info, err := os.Stat(name)
	switch {
	case os.IsNotExist(err): // File doesn't exist yet => create it
		f, err := os.Create(name)
		if err != nil {
			return stats, err
		}
		f.Close()
		isBlank = true
	case err != nil: // Some other error => bail
		return stats, err
	case isDevice(info.Mode()): // Dealing with a block device
		isBlkDevice = true
	case info.Size() == 0: // Is a file that exists, but is empty => use optimizations for blank files
		isBlank = true
	}

	// Truncate the output file to the full expected size. Not only does this
	// confirm there's enough disk space, but it allows for an optimization
	// when dealing with the Null Chunk
	if !isBlkDevice {
		if err := os.Truncate(name, idx.Length()); err != nil {
			return stats, err
		}
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
	seeds = append([]Seed{ns}, seeds...)

	// Start a self-seed which will become usable once chunks are written contigously
	// beginning at position 0. There is no need to add this to the seeds list because
	// when we create a plan it will be empty.
	ss, err := newSelfSeed(name, idx)
	if err != nil {
		return stats, err
	}

	// Record the total number of seeds and blocksize in the stats
	stats.Seeds = len(seeds)
	stats.Blocksize = blocksize

	// Start the workers, each having its own filehandle to write concurrently
	for i := 0; i < options.N; i++ {
		f, err := os.OpenFile(name, os.O_RDWR, 0666)
		if err != nil {
			return stats, fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()
		g.Go(func() error {
			for job := range in {
				if pb != nil {
					pb.Add(job.segment.lengthChunks())
				}
				if job.source != nil {
					// If we have a seedSegment we expect 1 or more chunks between
					// the start and the end of this segment.
					stats.addChunksFromSeed(uint64(job.segment.lengthChunks()))
					offset := job.segment.start()
					length := job.segment.lengthBytes()
					copied, cloned, err := job.source.WriteInto(f, offset, length, blocksize, isBlank)
					if err != nil {
						return err
					}

					// Validate that the written chunks are exactly what we were expecting.
					// Because the seed might point to a RW location, if the data changed
					// while we were extracting an index, we might end up writing to the
					// destination some unexpected values.
					for _, c := range job.segment.chunks() {
						b := make([]byte, c.Size)
						if _, err := f.ReadAt(b, int64(c.Start)); err != nil {
							return err
						}
						sum := Digest.Sum(b)
						if sum != c.ID {
							return fmt.Errorf("written data in %s doesn't match its expected hash value, seed may have changed during processing", name)
						}
					}

					stats.addBytesCopied(copied)
					stats.addBytesCloned(cloned)
					// Record this segment's been written in the self-seed to make it
					// available going forward
					ss.add(job.segment)
					continue
				}

				// If we don't have a seedSegment we expect an IndexSegment with just
				// a single chunk, that we can take from either the selfSeed, from the
				// destination file, or from the store.
				if len(job.segment.chunks()) != 1 {
					panic("Received an unexpected segment that doesn't contain just a single chunk")
				}
				c := job.segment.chunks()[0]

				// If we already took this chunk from the store we can reuse it by looking
				// into the selfSeed.
				if segment := ss.getChunk(c.ID); segment != nil {
					copied, cloned, err := segment.WriteInto(f, c.Start, c.Size, blocksize, isBlank)
					if err != nil {
						return err
					}
					stats.addBytesCopied(copied)
					stats.addBytesCloned(cloned)
					// Even if we already confirmed that this chunk is present in the
					// self-seed, we still need to record it as being written, otherwise
					// the self-seed position pointer doesn't advance as we expect.
					ss.add(job.segment)
				}

				// If we operate on an existing file there's a good chance we already
				// have the data written for this chunk. Let's read it from disk and
				// compare to what is expected.
				if !isBlank {
					b := make([]byte, c.Size)
					if _, err := f.ReadAt(b, int64(c.Start)); err != nil {
						return err
					}
					sum := Digest.Sum(b)
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
				chunk, err := s.GetChunk(c.ID)
				if err != nil {
					return err
				}
				b, err := chunk.Data()
				if err != nil {
					return err
				}
				// Might as well verify the chunk size while we're at it
				if c.Size != uint64(len(b)) {
					return fmt.Errorf("unexpected size for chunk %s", c.ID)
				}
				// Write the decompressed chunk into the file at the right position
				if _, err = f.WriteAt(b, int64(c.Start)); err != nil {
					return err
				}
				// Record this chunk's been written in the self-seed
				ss.add(job.segment)
			}
			return nil
		})
	}

	// Let the sequencer break up the index into segments, create and validate a plan,
	// feed the workers, and stop if there are any errors
	seq := NewSeedSequencer(idx, seeds...)
	plan := seq.Plan()
	for {
		if err := plan.Validate(ctx, options.N); err != nil {
			// This plan has at least one invalid seed
			switch options.InvalidSeedAction {
			case InvalidSeedActionBailOut:
				return stats, err
			case InvalidSeedActionRegenerate:
				Log.WithError(err).Info("Unable to use one of the chosen seeds, regenerating it")
				if err := seq.RegenerateInvalidSeeds(ctx, options.N); err != nil {
					return stats, err
				}
			case InvalidSeedActionSkip:
				// Recreate the plan. This time the seed marked as invalid will be skipped
				Log.WithError(err).Info("Unable to use one of the chosen seeds, skipping it")
			default:
				panic("Unhandled InvalidSeedAction")
			}

			seq.Rewind()
			plan = seq.Plan()
			continue
		}
		// Found a valid plan
		break
	}

loop:
	for _, segment := range plan {
		select {
		case <-ctx.Done():
			break loop
		case in <- Job{segment.indexSegment, segment.source}:
		}
	}
	close(in)

	return stats, g.Wait()
}
