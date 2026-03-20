package desync

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
)

type PlanOption func(*AssemblePlan)

func PlanWithConcurrency(n int) PlanOption {
	return func(p *AssemblePlan) {
		p.concurrency = n
	}
}

func PlanWithSeeds(seeds []Seed) PlanOption {
	return func(p *AssemblePlan) {
		p.seeds = seeds
	}
}

func PlanWithTargetIsBlank(isBlank bool) PlanOption {
	return func(p *AssemblePlan) {
		p.targetIsBlank = isBlank
	}
}

// AssemblePlan holds a directed acyclic graph of steps.
type AssemblePlan struct {
	idx           Index
	concurrency   int
	target        string
	store         Store
	seeds         []Seed
	targetIsBlank bool

	// Placements is an intermediate representation of the target index,
	// capturing what source is used to populate each chunk. It mirrors the
	// length of the index but a single step can span multiple chunks.
	placements []*placement

	// InPlaceReads is a list of placements with sources that read sections
	// from the target file. This needs to happen before any steps that
	// overwrite the in-place source data. This is sparsely populated and
	// used to express a dependency in the form "don't write to this chunk
	// until these chunks are read from the in-place target".
	inPlaceReads []*placement

	selfSeed *selfSeed
}

type assembleSource interface {
	fmt.Stringer
	Execute(f *os.File) (copied uint64, cloned uint64, err error)
}

type assembleSeedSource interface {
	assembleSource
	Seed() Seed
	File() string
	Validate(file *os.File) error
}

type placement struct {
	source         assembleSource
	dependsOnStart int // index of another placement this one depends on
	dependsOnSize  int // number of sequential placements (from dependsOnStart) this depends on
}

// NewPlan creates a fully populated AssemblePlan.
func NewPlan(name string, idx Index, s Store, opts ...PlanOption) (*AssemblePlan, error) {
	p := &AssemblePlan{
		idx:           idx,
		concurrency:   1,
		target:        name,
		store:         s,
		targetIsBlank: true,
		placements:    make([]*placement, len(idx.Chunks)),
		inPlaceReads:  make([]*placement, len(idx.Chunks)),
	}
	for _, opt := range opts {
		opt(p)
	}

	ss, err := newSelfSeed(p.target, p.idx, p.concurrency)
	if err != nil {
		return nil, err
	}
	p.selfSeed = ss

	if err := p.generate(); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Close releases resources held by the plan.
func (p *AssemblePlan) Close() {
	if p.selfSeed != nil {
		p.selfSeed.Close()
	}
}

// Validate checks that all file seed placements still match their underlying
// data. Returns a SeedInvalid error if a seed file was modified after its
// index was created.
func (p *AssemblePlan) Validate() error {
	// Phase 1 — Sequential: collect unique fileSeedSource placements, open
	// their backing files, and build a list of items to validate.
	type validateItem struct {
		fs   assembleSeedSource
		file *os.File
	}

	seen := make(map[*placement]struct{})
	fileMap := make(map[string]*os.File)
	defer func() {
		for _, f := range fileMap {
			f.Close()
		}
	}()

	invalidSeeds := make(map[Seed]error)
	failedFiles := make(map[string]struct{})

	var items []validateItem
	for _, pl := range p.placements {
		if _, ok := seen[pl]; ok {
			continue
		}
		seen[pl] = struct{}{}

		fs, ok := pl.source.(assembleSeedSource)
		if !ok || fs.File() == "" {
			continue
		}

		// Skip seeds and files already known to be invalid
		if _, ok := invalidSeeds[fs.Seed()]; ok {
			continue
		}
		if _, ok := failedFiles[fs.File()]; ok {
			invalidSeeds[fs.Seed()] = fmt.Errorf("seed file %s could not be opened", fs.File())
			continue
		}

		if _, ok := fileMap[fs.File()]; !ok {
			f, err := os.Open(fs.File())
			if err != nil {
				failedFiles[fs.File()] = struct{}{}
				invalidSeeds[fs.Seed()] = err
				continue
			}
			fileMap[fs.File()] = f
		}

		items = append(items, validateItem{fs: fs, file: fileMap[fs.File()]})
	}

	// Phase 2 — Concurrent: validate each segment in parallel.
	var mu sync.Mutex
	var g errgroup.Group
	g.SetLimit(p.concurrency)
	for _, item := range items {
		g.Go(func() error {
			if err := item.fs.Validate(item.file); err != nil {
				mu.Lock()
				invalidSeeds[item.fs.Seed()] = err
				mu.Unlock()
			}
			return nil
		})
	}
	g.Wait()

	// Phase 3 — Sequential: build the error result.
	if len(invalidSeeds) > 0 {
		seeds := make([]Seed, 0, len(invalidSeeds))
		errs := make([]error, 0, len(invalidSeeds))
		for seed, err := range invalidSeeds {
			seeds = append(seeds, seed)
			errs = append(errs, err)
		}
		return SeedInvalid{Seeds: seeds, Err: errors.Join(errs...)}
	}
	return nil
}

func (p *AssemblePlan) generate() error {
	// Mark chunks that are already correct in the target file so they can
	// be skipped during assembly.
	if !p.targetIsBlank {
		f, err := os.Open(p.target)
		if err == nil {
			var g errgroup.Group
			g.SetLimit(p.concurrency)
			for i, chunk := range p.idx.Chunks {
				g.Go(func() error {
					b := make([]byte, chunk.Size)
					if _, err := f.ReadAt(b, int64(chunk.Start)); err != nil {
						return nil
					}
					if Digest.Sum(b) == chunk.ID {
						p.placements[i] = &placement{source: &skipInPlace{
							start: chunk.Start,
							end:   chunk.Start + chunk.Size,
						}}
					}
					return nil
				})
			}
			g.Wait()
			f.Close()

			// Merge consecutive in-place chunks into a single placement
			// so that Steps() produces one step per run instead of one
			// per chunk. This works because Steps() deduplicates by
			// pointer identity.
			var run *placement
			for i, pl := range p.placements {
				if pl == nil {
					run = nil
					continue
				}
				if _, ok := pl.source.(*skipInPlace); !ok {
					run = nil
					continue
				}
				if run == nil {
					run = pl
					continue
				}
				// Extend the existing run and share the pointer
				run.source.(*skipInPlace).end = p.idx.Chunks[i].Start + p.idx.Chunks[i].Size
				p.placements[i] = run
			}
		}
	}

	// If we have an in-place seed, use it to find matches in the file
	// before anything gets overwritten by subsequent steps. We schedule
	// steps that re-arrange chunks that already exist in other places in
	// the target file before they get overwritten by subsequent steps like
	// copying from other seeds or the store.
	for _, seed := range p.seeds {
		inPlaceSeed, ok := seed.(*InPlaceSeed)
		if !ok {
			continue
		}

		_ = inPlaceSeed

		// TODO: Implement finding chunk slices in the existing file.
		// Create placements with sources that consist of multiple
		// operations, such as "Copy to other spot in the file", "Read
		// to memory", "Write memory to file". Each dependency cycle is
		// represented as multiple operations. Each cycle can be
		// executed independently of other (disconnected) cycles.
		// Update inPlaceReads placements so subsequent steps that
		// write to the same sections in the file from other seeds or
		// store happen after the selfseed operations are done.
		for i := 0; i < len(p.idx.Chunks); i++ {
			byteOffset, byteLength, seedOffset, n := inPlaceSeed.LongestMatchFrom(p.idx.Chunks, i)
			if n < 1 {
				continue
			}

			_ = byteOffset
			_ = byteLength
			_ = seedOffset
		}

		break // There can only be one in-place seed
	}

	// Find all matches in file itself as they're written. As it's
	// populated, sections can be copied to other chunks. This involves
	// depending on earlier steps before chunks can be copied within the
	// file.
	for i := 0; i < len(p.idx.Chunks); i++ {
		if p.placements[i] != nil {
			continue // Already filled
		}

		_, _, start, n := p.selfSeed.LongestMatchFrom(p.idx.Chunks, i)
		if n < 1 {
			continue
		}

		// Repeat the same placement for all chunks in the sequence.
		// We dedup sequences later.
		pl := &placement{}

		// We can use up to n chunks from the seed, find out how much
		// we can actually use without overwriting any existing placements
		// in the list.
		var (
			to   = i
			size int
		)
		for range n {
			if p.placements[i] != nil {
				break
			}

			p.placements[i] = pl
			i++
			size++
		}
		i-- // compensate for the outer loop's i++

		// Update the step with the potentially adjusted length
		seedOffset := p.idx.Chunks[start].Start
		last := p.idx.Chunks[start+size-1]
		length := last.Start + last.Size - seedOffset
		offset := p.idx.Chunks[to].Start

		pl.source = p.selfSeed.GetSegment(seedOffset, offset, length)
		pl.dependsOnStart = start
		pl.dependsOnSize = size
	}

	// Check file seeds for matches in unfilled positions.
	for _, seed := range p.seeds {
		if _, ok := seed.(*InPlaceSeed); ok { // Skip the in-place seed, it's already handled
			continue
		}

		for i := 0; i < len(p.idx.Chunks); i++ {
			if p.placements[i] != nil {
				continue
			}

			seedOffset, _, _, n := seed.LongestMatchFrom(p.idx.Chunks, i)
			if n < 1 {
				continue
			}

			// Repeat the same placement for all chunks in the sequence.
			// We dedup sequences later.
			pl := &placement{}

			// We can use up to n chunks from the seed, find out how much
			// we can actually use without overwriting any existing placements
			// in the list.
			var (
				to   = i
				size int
			)
			for range n {
				if p.placements[i] != nil {
					break
				}
				p.placements[i] = pl
				i++
				size++
			}
			i-- // compensate for the outer loop's i++

			// Update the step with the potentially adjusted length
			offset := p.idx.Chunks[to].Start
			last := p.idx.Chunks[to+size-1]
			length := last.Start + last.Size - offset
			segment := seed.GetSegment(seedOffset, length)

			pl.source = &fileSeedSource{
				segment: segment,
				seed:    seed,
				srcFile: segment.FileName(),
				offset:  offset,
				length:  length,
				isBlank: p.targetIsBlank,
			}
		}
	}

	// Fill any gaps in the file by copying from the store.
	for i := range p.placements {
		if p.placements[i] != nil {
			continue
		}
		p.placements[i] = &placement{
			source: &copyFromStore{
				store: p.store,
				chunk: p.idx.Chunks[i],
			},
		}
	}

	// We now have a fully populated list of placements. Some are
	// duplicates, spanning multiple chunks. Dependencies are only defined
	// forward, like chunk-A needs chunk-B to be written first, etc.

	return nil
}

func (p *AssemblePlan) Steps() []*PlanStep {
	// Create a step for every unique placement, counting how many
	// index chunks each step covers.
	stepsPerPlacement := make(map[*placement]*PlanStep)
	for _, pl := range p.placements {
		step, ok := stepsPerPlacement[pl]
		if !ok {
			step = &PlanStep{
				source: pl.source,
			}
			stepsPerPlacement[pl] = step
		}
		step.numChunks++
	}

	// Link the steps together. Use a seen set to avoid redundant work
	// when the same placement pointer spans multiple chunks.
	linked := make(map[*placement]struct{}, len(stepsPerPlacement))
	for _, pl := range p.placements {
		if _, ok := linked[pl]; ok {
			continue
		}
		linked[pl] = struct{}{}

		for i := pl.dependsOnStart; i < pl.dependsOnStart+pl.dependsOnSize; i++ {
			stepsPerPlacement[pl].addDependency(stepsPerPlacement[p.placements[i]])
			stepsPerPlacement[p.placements[i]].addDependent(stepsPerPlacement[pl])
		}

		// TODO: setup dependencies on inPlaceReads to make sure
		// in-seed operations on chunks are done before subsequent
		// writes overwrite the data.
	}

	// Make a slice of steps, preserving the order
	steps := make([]*PlanStep, 0, len(stepsPerPlacement))
	for _, pl := range p.placements {
		s, ok := stepsPerPlacement[pl]
		if !ok {
			continue
		}
		steps = append(steps, s)
		delete(stepsPerPlacement, pl)
	}

	return steps
}
