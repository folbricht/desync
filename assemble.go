package desync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"golang.org/x/sync/errgroup"
)

// InvalidSeedAction represents the action that we will take if a seed
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
func AssembleFile(ctx context.Context, name string, idx Index, s Store, seeds []Seed, options AssembleOptions) (*ExtractStats, error) {
	var (
		isBlank     bool
		isBlkDevice bool
		attempt     = 1
	)

	// Initialize stats to be gathered during extraction
	stats := &ExtractStats{
		BytesTotal:  idx.Length(),
		ChunksTotal: len(idx.Chunks),
	}

	// Determine if the target exists and create it if not
	info, err := os.Stat(name)
	switch {
	case os.IsNotExist(err): // File doesn't exist yet => create it
		f, err := os.Create(name)
		if err != nil {
			return stats, err
		}
		if err := f.Close(); err != nil {
			return stats, err
		}
		isBlank = true
	case err != nil: // Some other error => bail
		return stats, err
	case isDevice(info.Mode()): // Dealing with a block device
		isBlkDevice = true
	case info.Size() == 0: // Is a file that exists, but is empty => use optimizations for blank files
		isBlank = true
	}

	// Separate the in-place seed (if any) from the file seeds. It reads
	// from the file being assembled and is handed to the plan explicitly.
	// A file seed whose data file is the target can't be read from while
	// the target is being written, it's used in place instead. There can
	// only be one, extras are dropped.
	targetInfo, err := os.Stat(name)
	if err != nil {
		return stats, err
	}
	isTarget := func(file string) bool {
		info, err := os.Stat(file)
		return err == nil && os.SameFile(info, targetInfo)
	}
	var (
		inPlaceSeed *FileSeed
		fileSeeds   []Seed
	)
	for _, seed := range seeds {
		if fs, ok := seed.(*FileSeed); ok && isTarget(fs.srcFile) {
			if inPlaceSeed == nil {
				inPlaceSeed = fs
			}
			continue
		}
		fileSeeds = append(fileSeeds, seed)
	}

	// Truncate the output file to the full expected size. Not only does this
	// confirm there's enough disk space, but it allows for an optimization
	// when dealing with the Null Chunk. On Darwin, the file is physically
	// pre-allocated as well since sparse files on APFS have shown to cause
	// issues when written to concurrently. If the file is larger than the
	// output, shrinking it waits until after assembly as the data beyond the
	// end may still be moved into place. A target without chunks is
	// truncated right away as there is nothing to assemble.
	var size int64
	if info != nil {
		size = info.Size()
	}
	shrinkAfter := !isBlkDevice && size > idx.Length() && len(idx.Chunks) > 0
	if !isBlkDevice && !shrinkAfter {
		if err := preallocateFile(name, idx.Length()); err != nil {
			return stats, err
		}
	}

	// An index of a zero-length file has no chunks. The file has been created
	// (and truncated) above, so there is nothing left to do.
	if len(idx.Chunks) == 0 {
		return stats, nil
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
	fileSeeds = append([]Seed{ns}, fileSeeds...)

	// Record the total number of seeds and blocksize in the stats
	stats.Seeds = len(fileSeeds)
	if inPlaceSeed != nil {
		stats.Seeds++
	}
	stats.Blocksize = blocksize

	// Create the plan and validate the seed indexes. Regenerating or
	// skipping invalid seeds restarts planning with the modified seeds.
	plan := newPlan(name, idx, s,
		planWithConcurrency(options.N),
		planWithSeeds(fileSeeds),
		planWithInPlaceSeed(inPlaceSeed),
		planWithTargetIsBlank(isBlank),
		planWithBlocksize(blocksize),
		planWithBufferBudget(inPlaceBufferBudget()),
	)
	for {
		err := plan.Validate()
		if err == nil {
			break
		}
		var seedError SeedInvalid
		if !errors.As(err, &seedError) {
			return stats, err
		}

		switch options.InvalidSeedAction {
		case InvalidSeedActionBailOut:
			return stats, err
		case InvalidSeedActionRegenerate:
			Log.WithError(err).Info("Unable to use one or more seeds, regenerating them")
			for i, s := range seedError.Seeds {
				if err := s.RegenerateIndex(ctx, options.N, attempt, i+1); err != nil {
					return stats, err
				}
			}
			attempt++
		case InvalidSeedActionSkip:
			Log.WithError(err).Infof("Unable to use one or more seeds, skipping them")
			if inPlaceSeed != nil && slices.Contains(seedError.Seeds, Seed(inPlaceSeed)) {
				inPlaceSeed = nil
			}
			fileSeeds = slices.DeleteFunc(fileSeeds, func(s Seed) bool {
				return slices.Contains(seedError.Seeds, s)
			})
		default:
			panic("Unhandled InvalidSeedAction")
		}
		plan = plan.replan(planWithSeeds(fileSeeds), planWithInPlaceSeed(inPlaceSeed))
	}

	// Generate the plan steps necessary to build the target
	steps := plan.Steps()
	if len(steps) == 0 {
		return stats, nil
	}

	// Set up progress bar
	pb := NewProgressBar(fmt.Sprintf("Attempt %d: Assembling ", attempt))
	pb.SetTotal(len(idx.Chunks))
	pb.Start()
	defer pb.Finish()

	// Workers take steps that can run from one channel and report them on
	// another once complete.
	var (
		work      = make(chan *planStep)
		completed = make(chan *planStep)
	)

	g, ctx := errgroup.WithContext(ctx)

	// Bring up the workers
	for range options.N {
		g.Go(func() error {
			f, err := os.OpenFile(name, os.O_RDWR, 0666)
			if err != nil {
				return fmt.Errorf("unable to open file %s, %s", name, err)
			}
			defer f.Close()
			for step := range work {
				copied, cloned, err := step.execute(f)
				if err != nil {
					return err
				}
				// Update byte-level stats
				stats.addBytesCopied(copied)
				stats.addBytesCloned(cloned)
				// Update chunk-level stats
				step.source.recordStats(stats, step.numChunks)
				select {
				case completed <- step:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	// Hand out the steps as they become ready, until all are complete or
	// a worker failed.
	var queue stepQueue
	for _, step := range steps {
		if step.ready() {
			queue.push(step)
		}
	}
dispatch:
	for remaining := len(steps); remaining > 0; {
		next := queue.peek()
		var out chan<- *planStep
		if next != nil {
			out = work
		}
		select {
		case out <- next:
			queue.pop()
		case step := <-completed:
			remaining--
			pb.Add(step.numChunks)

			// Remove the dependency from the steps blocked by this one,
			// queueing those that have no more dependencies.
			for b := range step.dependents {
				delete(b.dependencies, step)
				if b.ready() {
					queue.push(b)
				}
			}
		case <-ctx.Done():
			break dispatch
		}
	}
	close(work)

	// Wait for the workers to complete
	err = g.Wait()

	// A seed file that was written to while it was read may have been copied
	// into the target in its new state. Check the output and take the chunks
	// that don't match from the store.
	if err == nil {
		if changed := plan.changedSeeds(); len(changed) > 0 {
			Log.WithField("seeds", changed).Warn("Seeds changed during assembly, verifying the output")
			if err := backfill(name, idx, s, options.N, stats); err != nil {
				return stats, err
			}
		}
	}

	// Shrink the file now that all in-place reads are complete.
	if err == nil && shrinkAfter {
		if err := os.Truncate(name, idx.Length()); err != nil {
			return stats, err
		}
	}

	return stats, err
}
