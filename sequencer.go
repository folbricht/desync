package desync

// SeedSequencer is used to find sequences of chunks from seed files when assembling
// a file from an index. Using seeds reduces the need to download and decompress chunks
// from chunk stores. It also enables the use of reflinking/cloning of sections of
// files from a seed file where supported to reduce disk usage.
type SeedSequencer struct {
	seeds   []Seed
	index   Index
	current int
}

// NewSeedSequencer initializes a new sequencer from a number of seeds.
func NewSeedSequencer(idx Index, src ...Seed) *SeedSequencer {
	return &SeedSequencer{
		seeds: src,
		index: idx,
	}
}

// Next returns a sequence of index chunks (from the target index) and the
// longest matching segment from one of the seeds. If source is nil, no
// match was found in the seeds and the chunk needs to be retrieved from a
// store. If done is true, the sequencer is complete.
func (r *SeedSequencer) Next() (segment IndexSegment, source SeedSegment, done bool) {
	var (
		max     uint64
		advance = 1
	)
	for _, s := range r.seeds {
		n, m := s.LongestMatchWith(r.index.Chunks[r.current:])
		if n > 0 && m.Size() > max {
			source = m
			advance = n
			max = m.Size()
		}
	}

	segment = IndexSegment{index: r.index, first: r.current, last: r.current + advance - 1}
	r.current += advance
	return segment, source, r.current >= len(r.index.Chunks)
}
