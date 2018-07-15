package desync

type SeedSequencer struct {
	seeds   []Seed
	index   Index
	current int
}

func NewSeedSequencer(idx Index, src ...Seed) *SeedSequencer {
	return &SeedSequencer{
		seeds: src,
		index: idx,
	}
}

func (r *SeedSequencer) Next() (indexSegment, SeedSegment, bool) {
	var (
		max     uint64
		advance int = 1
		source  SeedSegment
	)
	for _, s := range r.seeds {
		n, m := s.LongestMatchWith(r.index.Chunks[r.current:])
		if n > 0 && m.Size() > max {
			source = m
			advance = n
			max = m.Size()
		}
	}

	segment := indexSegment{index: r.index, first: r.current, last: r.current + advance - 1}
	r.current += advance
	return segment, source, r.current >= len(r.index.Chunks)
}
