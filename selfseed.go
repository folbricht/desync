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
	cache      map[int]int
}

// newSelfSeed initializes a new seed based on the file being extracted
func newSelfSeed(file string, index Index) (*selfSeed, error) {
	s := selfSeed{
		file:       file,
		pos:        make(map[ChunkID][]int),
		index:      index,
		canReflink: CanClone(file, file),
		cache:      make(map[int]int),
	}
	return &s, nil
}

// add records a new segment that's been written to the file. Since only contiguous
// ranges of chunks are considered and writing happens concurrently, the segment
// written here will not be usable until all earlier chunks have been written as
// well.
func (s *selfSeed) add(segment IndexSegment) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make a record of this segment in the cache since those could come in
	// out-of-order
	s.cache[segment.first] = segment.last + 1

	// Advance pos until we find a chunk we don't yet have recorded while recording
	// the chunk positions we do have in the position map used to find seed matches.
	// Since it's guaranteed that the numbers are only increasing, we drop old numbers
	// from the cache map to keep it's size to a minimum and only store out-of-sequence
	// numbers
	for {
		// See if we can advance the write pointer in the self-seed which requires
		// consecutive chunks. If we don't have the next segment yet, just keep it
		// in the cache until we do.
		next, ok := s.cache[s.written]
		if !ok {
			break
		}
		// Record all chunks in this segment as written by adding them to the position map
		for i := s.written; i < next; i++ {
			chunk := s.index.Chunks[i]
			s.pos[chunk.ID] = append(s.pos[chunk.ID], i)
		}
		delete(s.cache, s.written)
		s.written = next
	}
}

// getChunk returns a segment with the requested chunk ID. If selfSeed doesn't
// have the requested chunk, nil will be returned.
func (s *selfSeed) getChunk(id ChunkID) SeedSegment {
	s.mu.RLock()
	pos, ok := s.pos[id]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	first := pos[0]
	return newFileSeedSegment(s.file, s.index.Chunks[first:first+1], s.canReflink)
}

func (s *selfSeed) SetInvalid(value bool) {
	panic("A selfSeed is never expected to be invalid")
}
