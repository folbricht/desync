package desync

import (
	"context"
	"golang.org/x/sync/errgroup"
	"os"
)

// SeedSequencer is used to find sequences of chunks from seed files when assembling
// a file from an index. Using seeds reduces the need to download and decompress chunks
// from chunk stores. It also enables the use of reflinking/cloning of sections of
// files from a seed file where supported to reduce disk usage.
type SeedSequencer struct {
	seeds   []Seed
	index   Index
	current int
}

// SeedSegmentCandidate represent a single segment that we expect to use
// in a Plan
type SeedSegmentCandidate struct {
	seed         Seed
	source       SeedSegment
	indexSegment IndexSegment
}

type Plan []SeedSegmentCandidate

// NewSeedSequencer initializes a new sequencer from a number of seeds.
func NewSeedSequencer(idx Index, src ...Seed) *SeedSequencer {
	return &SeedSequencer{
		seeds: src,
		index: idx,
	}
}

// Plan returns a new possible plan, representing an ordered list of
// segments that can be used to re-assemble the requested file
func (r *SeedSequencer) Plan() (plan Plan) {
	for {
		seed, segment, source, done := r.Next()
		plan = append(plan, SeedSegmentCandidate{seed, source, segment})
		if done {
			break
		}
	}
	return plan
}

// Next returns a sequence of index chunks (from the target index) and the
// longest matching segment from one of the seeds. If source is nil, no
// match was found in the seeds and the chunk needs to be retrieved from a
// store. If done is true, the sequencer is complete.
func (r *SeedSequencer) Next() (seed Seed, segment IndexSegment, source SeedSegment, done bool) {
	var (
		max     uint64
		advance = 1
	)
	for _, s := range r.seeds {
		n, m := s.LongestMatchWith(r.index.Chunks[r.current:])
		if n > 0 && m.Size() > max {
			seed = s
			source = m
			advance = n
			max = m.Size()
		}
	}

	segment = IndexSegment{index: r.index, first: r.current, last: r.current + advance - 1}
	r.current += advance
	return seed, segment, source, r.current >= len(r.index.Chunks)
}

// Rewind resets the current target index to the beginning.
func (r *SeedSequencer) Rewind() {
	r.current = 0
}

//isFileSeed returns true if this segment is pointing to a fileSeed
func (s SeedSegmentCandidate) isFileSeed() bool {
	// We expect an empty filename when using nullSeeds
	return s.source != nil && s.source.FileName() != ""
}

// RegenerateInvalidSeeds regenerates the index to match the unexpected seed content
func (r *SeedSequencer) RegenerateInvalidSeeds(ctx context.Context, n int) error {
	for _, s := range r.seeds {
		if s.IsInvalid() {
			if err := s.RegenerateIndex(ctx, n); err != nil {
				return err
			}
		}
	}
	return nil
}

// Validate validates a proposed plan by checking if all the chosen chunks
// are correctly provided from the seeds. In case a seed has invalid chunks, the
// entire seed is marked as invalid and an error is returned.
func (p Plan) Validate(ctx context.Context, n int, pb ProgressBar) (err error) {
	type Job struct {
		candidate SeedSegmentCandidate
		file      *os.File
	}
	var (
		in      = make(chan Job)
		fileMap = make(map[string]*os.File)
	)
	length := 0
	for _, s := range p {
		if !s.isFileSeed() {
			continue
		}
		length += s.indexSegment.lengthChunks()
	}
	pb.SetTotal(length)
	pb.Start()
	defer pb.Finish()
	// Share a single file descriptor per seed for all the goroutines
	for _, s := range p {
		if !s.isFileSeed() {
			continue
		}
		name := s.source.FileName()
		if _, present := fileMap[name]; present {
			continue
		} else {
			file, err := os.Open(name)
			if err != nil {
				// We were not able to open the seed. Mark it as invalid and return
				s.seed.SetInvalid(true)
				return err
			}
			fileMap[name] = file
			defer file.Close()
		}
	}
	g, ctx := errgroup.WithContext(ctx)
	// Concurrently validate all the chunks in this plan
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for job := range in {
				if err := job.candidate.source.Validate(job.file); err != nil {
					job.candidate.seed.SetInvalid(true)
					return err
				}
				pb.Add(job.candidate.indexSegment.lengthChunks())
			}
			return nil
		})
	}

loop:
	for _, s := range p {
		if !s.isFileSeed() {
			// This is not a fileSeed, we have nothing to validate
			continue
		}
		select {
		case <-ctx.Done():
			break loop
		case in <- Job{s, fileMap[s.source.FileName()]}:
		}
	}
	close(in)

	return g.Wait()
}
