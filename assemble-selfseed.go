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
	canReflink bool
	blocksize  uint64
}

func newSelfSeed(file string, index Index) *selfSeed {
	s := &selfSeed{
		file:       file,
		seedIndex:  newSeedIndex(index),
		canReflink: CanClone(file, file),
		blocksize:  blocksizeOfFile(file),
	}
	// Zero chunks are handled better by the null seed: writing zeros to a
	// blank target is a no-op there, while a self-copy of zeros is real
	// read/write I/O that also chains dependencies across the whole run.
	// Remove the null chunk from the lookup so those positions fall through
	// to the null seed.
	delete(s.pos, NewNullChunk(index.Index.ChunkSizeMax).ID)
	return s
}

// LongestMatchFrom returns the longest sequence of matching chunks after a
// given starting position. It returns the chunk position of the match and
// the number of matching chunks, or (0, 0) if there is no match. Only
// positions after startPos are considered, and matches are clamped so the
// source and destination ranges can't overlap.
func (s *selfSeed) LongestMatchFrom(chunks []IndexChunk, startPos int) (int, int) {
	return s.longestMatchFrom(chunks, startPos, s.canReflink, selfSeedMaxCandidates, func(p int) int {
		if p <= startPos {
			return 0
		}
		// The source run [p, p+n) may not overlap the destination run
		// [startPos, startPos+n), which limits a usable run to the distance
		// between the two.
		return p - startPos
	})
}

func (s *selfSeed) GetSegment(srcOffset, dstOffset, size uint64) *selfSeedSegment {
	return &selfSeedSegment{
		seed:      s,
		srcOffset: srcOffset,
		dstOffset: dstOffset,
		size:      size,
	}
}

type selfSeedSegment struct {
	seed      *selfSeed
	srcOffset uint64
	dstOffset uint64
	size      uint64
}

func (s *selfSeedSegment) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	// The source is the file being assembled. Open a separate read handle,
	// like fileSeedSegment does for its seed file, so reads don't interfere
	// with the worker's write handle. The source and destination ranges
	// never overlap, the plan clamps matches to prevent that.
	src, err := os.Open(s.seed.file)
	if err != nil {
		return 0, 0, err
	}
	defer src.Close()

	blocksize := s.seed.blocksize

	// Use reflinks if supported and blocks are aligned
	if s.seed.canReflink && s.srcOffset%blocksize == s.dstOffset%blocksize {
		return cloneOrCopyRange(f, src, s.srcOffset, s.size, s.dstOffset, blocksize)
	}
	return copyRange(f, src, s.srcOffset, s.size, s.dstOffset)
}

func (s *selfSeedSegment) String() string {
	return fmt.Sprintf("SelfSeed: Copy [%d:%d] to [%d:%d]",
		s.srcOffset, s.srcOffset+s.size,
		s.dstOffset, s.dstOffset+s.size)
}
