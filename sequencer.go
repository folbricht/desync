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

func (r *SeedSequencer) Next() (to []IndexChunk, from section, done bool) {
	var (
		max     uint64
		advance int = 1
	)
	for _, s := range r.seeds {
		n, m := s.longestMatchWith(r.index.Chunks[r.current:])
		if n > 0 && m.size() > max {
			from = m
			advance = n
			max = m.size()
		}
	}

	to = r.index.Chunks[r.current : r.current+advance]
	r.current += advance
	return to, from, r.current >= len(r.index.Chunks)
}
