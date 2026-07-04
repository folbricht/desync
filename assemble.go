package desync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"

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
		f.Close()
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
	// There can only be one, extras are dropped.
	var (
		inPlaceSeed *InPlaceSeed
		fileSeeds   []Seed
	)
	for _, seed := range seeds {
		if ips, ok := seed.(*InPlaceSeed); ok {
			if inPlaceSeed == nil {
				inPlaceSeed = ips
			}
			continue
		}
		fileSeeds = append(fileSeeds, seed)
	}

	// The in-place seed size (if any) decides the truncation strategy.
	var inPlaceSeedSize int64
	if inPlaceSeed != nil {
		inPlaceSeedSize = inPlaceSeed.index.Length()
	}

	// Truncate the output file to the full expected size. Not only does this
	// confirm there's enough disk space, but it allows for an optimization
	// when dealing with the Null Chunk. If the in-place seed is larger than
	// the target, defer truncation until after assembly so in-place reads
	// can access the tail data.
	if !isBlkDevice {
		if inPlaceSeedSize <= idx.Length() {
			if err := os.Truncate(name, idx.Length()); err != nil {
				return stats, err
			}
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
	fileSeeds = append([]Seed{ns}, fileSeeds...)

	// Record the total number of seeds and blocksize in the stats
	stats.Seeds = len(fileSeeds)
	if inPlaceSeed != nil {
		stats.Seeds++
	}
	stats.Blocksize = blocksize

	// Create the plan and validate the seed indexes. Regenerating or
	// skipping invalid seeds restarts planning with the modified seeds.
	var plan *AssemblePlan
	for {
		plan = NewPlan(name, idx, s,
			PlanWithConcurrency(options.N),
			PlanWithSeeds(fileSeeds),
			PlanWithInPlaceSeed(inPlaceSeed),
			PlanWithTargetIsBlank(isBlank),
		)

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
	}

	// Generate the plan steps necessary to build the target
	steps := plan.Steps()
	if len(steps) == 0 {
		return stats, nil
	}

	// Split the steps into those that are independent and those that
	// require other steps to complete first.
	var (
		ready   []*PlanStep
		delayed = make(map[*PlanStep]struct{})
	)
	for _, step := range steps {
		if step.ready() {
			ready = append(ready, step)
		} else {
			delayed[step] = struct{}{}
		}
	}

	// Set up progress bar
	pb := NewProgressBar(fmt.Sprintf("Attempt %d: Assembling ", attempt))
	pb.SetTotal(len(idx.Chunks))
	pb.Start()
	defer pb.Finish()

	// Create two channels, one for steps that can run and one for those
	// that are complete.
	var (
		inProgress = make(chan *PlanStep, len(steps))
		completed  = make(chan *PlanStep, options.N)
	)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(options.N)

	// Bring up the workers
	for range options.N {
		g.Go(func() error {
			f, err := os.OpenFile(name, os.O_RDWR, 0666)
			if err != nil {
				return fmt.Errorf("unable to open file %s, %s", name, err)
			}
			defer f.Close()
			for {
				select {
				case step, ok := <-inProgress:
					if !ok {
						return nil
					}
					copied, cloned, err := step.source.Execute(f)
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
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	// Populate all steps that are ready to be executed
	for _, step := range ready {
		inProgress <- step
	}

	// Start the dispatch goroutine which runs the plan. This should be
	// outside the errgroup as it'll only be stopped once the workers are
	// done.
	var wg sync.WaitGroup
	wg.Go(func() {
		for step := range completed {
			pb.Add(step.numChunks)

			// Go through all the steps that are blocked by this
			// one and remove the dependency. If all deps have been
			// removed, send them for processing and remove them
			// from the ready list.
			for b := range step.dependents {
				delete(b.dependencies, step)
				if b.ready() {
					delete(delayed, b)
					inProgress <- b
				}
			}

			// If there are no more delayed steps, close the work queue.
			if len(delayed) == 0 {
				close(inProgress)
				break
			}
		}

		// Drain the completed queue, updating the progress bar for any
		// steps that finished after the work queue was closed.
		for step := range completed {
			pb.Add(step.numChunks)
		}
	})

	// Wait for the workers to complete
	err = g.Wait()

	// Stop the dispatch goroutine
	close(completed)
	wg.Wait()

	// If the in-place seed was larger than the target, truncate now that
	// all in-place reads are complete.
	if err == nil && inPlaceSeedSize > idx.Length() && !isBlkDevice {
		if err := os.Truncate(name, idx.Length()); err != nil {
			return stats, err
		}
	}

	return stats, err
}
