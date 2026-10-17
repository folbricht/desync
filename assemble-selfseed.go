package desync

import (
	"fmt"
	"os"
)

// selfSeedMaxCandidates bounds the number of candidate positions the
// self-seed examines per match. Without a bound, chunks that repeat many
// times in the target make matching quadratic in the number of repetitions
// when reflinks are available (where run lengths aren't limited either).
const selfSeedMaxCandidates = 100

// selfSeed matches chunks against the file being assembled itself. Chunks
// that repeat in the target can be copied within the file once their first
// occurrence has been written.
type selfSeed struct {
	file string
	seedIndex
	blocksize uint64

	// inPlace looks up the chunks that are already in place in the target.
	// Nothing writes over them, so unlike the rest of the target they can
	// be copied from in either direction without waiting for other steps.
	// inPlaceRun holds the number of consecutive chunks in place from each
	// position.
	inPlace    seedIndex
	inPlaceRun []int
}

func newSelfSeed(file string, index Index, blocksize uint64) *selfSeed {
	s := &selfSeed{
		file:      file,
		seedIndex: newSeedIndex(index, CanClone(file, file)),
		blocksize: blocksize,
	}
	// Zero chunks are handled better by the null seed: writing zeros to a
	// blank target is a no-op there, while a self-copy of zeros is real
	// read/write I/O that also chains dependencies across the whole run.
	// Remove the null chunk from the lookup so those positions fall through
	// to the null seed.
	delete(s.pos, NewNullChunk(index.Index.ChunkSizeMax).ID)
	return s
}

// useInPlace records the chunks found in place in the target, given the
// skips of the plan, one per chunk position, nil where the chunk isn't in
// place.
func (s *selfSeed) useInPlace(skips []*skipInPlace) {
	s.inPlace = seedIndex{index: s.index, pos: make(map[ChunkID][]int), canReflink: s.canReflink}
	s.inPlaceRun = make([]int, len(skips))
	for i := len(skips) - 1; i >= 0; i-- {
		if skips[i] == nil {
			continue
		}
		s.inPlaceRun[i] = 1
		if i+1 < len(skips) {
			s.inPlaceRun[i] += s.inPlaceRun[i+1]
		}
	}
	for i, sk := range skips {
		id := s.index.Chunks[i].ID
		// Leave out the null chunk, just like the other positions: it's
		// the only chunk missing from s.pos.
		if _, ok := s.pos[id]; sk != nil && ok {
			s.inPlace.pos[id] = append(s.inPlace.pos[id], i)
		}
	}
}

// LongestMatchFrom returns the longest sequence of matching chunks for a
// given starting position. It returns the chunk position of the match and
// the number of matching chunks, or (0, 0) if there is no match. Positions
// after startPos are considered, with matches clamped so the source and
// destination ranges can't overlap, as well as runs of chunks already in
// place anywhere in the target.
func (s *selfSeed) LongestMatchFrom(chunks []IndexChunk, startPos int) (int, int) {
	pos, n := s.longestMatchFrom(chunks, startPos, startPos+1, selfSeedMaxCandidates, func(p int) int {
		// The source run [p, p+n) may not overlap the destination run
		// [startPos, startPos+n), which limits a usable run to the distance
		// between the two.
		return p - startPos
	})
	// Chunks in place before startPos are only found here, if the target
	// was checked for them. They're preferred over an equally long match
	// elsewhere, as reading them doesn't depend on any other step.
	inPos, inN := s.inPlace.longestMatchFrom(chunks, startPos, 0, selfSeedMaxCandidates, func(p int) int {
		run := s.inPlaceRun[p]
		if p > startPos {
			run = min(run, p-startPos)
		}
		return run
	})
	if inN > 0 && inN >= n {
		return inPos, inN
	}
	return pos, n
}

// GetSegment returns a segment copying n chunks starting at chunk position
// pos to dstOffset in the target.
func (s *selfSeed) GetSegment(pos, n int, dstOffset uint64) *selfSeedSegment {
	return &selfSeedSegment{
		segment:   newFileSeedSegment(s.file, s.index.Chunks[pos:pos+n], s.canReflink),
		dstOffset: dstOffset,
		blocksize: s.blocksize,
	}
}

type selfSeedSegment struct {
	segment   *fileSeedSegment
	dstOffset uint64
	blocksize uint64
}

// Execute copies the chunks within the file being assembled. The source and
// destination ranges never overlap, the plan clamps matches to prevent that.
func (s *selfSeedSegment) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	return s.segment.WriteInto(f, s.dstOffset, s.segment.Size(), s.blocksize, false)
}

// access reads what other steps write to the target during assembly.
func (s *selfSeedSegment) access() targetAccess {
	src, size := s.segment.chunks[0].Start, s.segment.Size()
	return targetAccess{
		writes: byteRange{s.dstOffset, s.dstOffset + size},
		reads:  byteRange{src, src + size},
	}
}

func (s *selfSeedSegment) recordStats(stats *ExtractStats, numChunks int) {
	stats.addChunksFromSeed(uint64(numChunks))
}

func (s *selfSeedSegment) String() string {
	src, size := s.segment.chunks[0].Start, s.segment.Size()
	return fmt.Sprintf("SelfSeed: Copy [%d:%d] to [%d:%d]",
		src, src+size, s.dstOffset, s.dstOffset+size)
}
