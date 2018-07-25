package desync

import (
	"sync"
)

// FileSeed is used to populate a contiguous seed during extraction in order
// to copy/clone ranges that were written to the output file earlier. This is
// to potentially dedup/reflink duplicate chunks or ranges of chunks within the
// same file.
type selfSeed struct {
	file       string
	index      Index
	pos        map[ChunkID][]int
	canReflink bool
	written    int
	mu         sync.RWMutex
	cache      map[int]struct{}
}

// newSelfSeed initializes a new seed based on the file being extracted
func newSelfSeed(file string, index Index) (*selfSeed, error) {
	s := selfSeed{
		file:       file,
		pos:        make(map[ChunkID][]int),
		index:      index,
		canReflink: CanClone(file, file),
		cache:      make(map[int]struct{}),
	}
	return &s, nil
}

// add records a new segment that's been written to the file. Since only contiguous
// ranges of chunks are considered and writing happens concurrently, the segment
// written here will not be usable until all earlier chunks have been written as
// well.
func (s *selfSeed) add(segment indexSegment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for n := segment.first; n <= segment.last; n++ {
		s.cache[n] = struct{}{}
	}
	// Advance pos until we find a chunk we don't yet have recorded while recording
	// the chunk positions we do have in the position map used to find seed matches.
	// Since it's guaranteed that the numbers are only increasing, we drop old numbers
	// from the cache map to keep it's size to a minimum and only store out-of-sequence
	// numbers
	for {
		if _, ok := s.cache[s.written]; !ok {
			break
		}
		chunk := s.index.Chunks[s.written]
		s.pos[chunk.ID] = append(s.pos[chunk.ID], s.written)
		delete(s.cache, s.written)
		s.written++
	}
}

// LongestMatchWith returns the longest sequence of of chunks anywhere in Source
// that match b starting at b[0]. If there is no match, it returns nil
func (s *selfSeed) LongestMatchWith(chunks []IndexChunk) (int, SeedSegment) {
	if len(chunks) == 0 || len(s.index.Chunks) == 0 {
		return 0, nil
	}
	s.mu.RLock()
	pos, ok := s.pos[chunks[0].ID]
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	// From every position of b[0] in the source, find a slice of
	// matching chunks. Then return the longest of those slices.
	var (
		match []IndexChunk
		max   int
	)
	for _, p := range pos {
		m := s.maxMatchFrom(chunks, p)
		if len(m) > max {
			match = m
			max = len(m)
		}
	}
	return max, newFileSeedSegment(s.file, match, s.canReflink, false)
}

// Returns a slice of chunks from the seed. Compares chunks from position 0
// with seed chunks starting at p.
func (s *selfSeed) maxMatchFrom(chunks []IndexChunk, p int) []IndexChunk {
	if len(chunks) == 0 {
		return nil
	}
	s.mu.RLock()
	written := s.written
	s.mu.RUnlock()
	var (
		sp int
		dp = p
	)
	for {
		if dp >= written || sp >= len(chunks) {
			break
		}
		if chunks[sp].ID != s.index.Chunks[dp].ID {
			break
		}
		dp++
		sp++
	}
	return s.index.Chunks[p:dp]
}
