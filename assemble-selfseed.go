package desync

import (
	"fmt"
	"os"
)

// selfSeed matches chunks against the file being assembled itself. Chunks
// that repeat in the target can be copied within the file once their first
// occurrence has been written.
type selfSeed struct {
	file string
	seedIndex
	canReflink bool
}

func newSelfSeed(file string, index Index) *selfSeed {
	return &selfSeed{
		file:       file,
		seedIndex:  newSeedIndex(index),
		canReflink: CanClone(file, file),
	}
}

// LongestMatchFrom returns the longest sequence of matching chunks after a
// given starting position. It returns the chunk position of the match and
// the number of matching chunks, or (0, 0) if there is no match. Only
// positions after startPos are considered, and matches are clamped so the
// source and destination ranges can't overlap.
func (s *selfSeed) LongestMatchFrom(chunks []IndexChunk, startPos int) (int, int) {
	return s.longestMatchFrom(chunks, startPos, s.canReflink, func(p, n int) int {
		if p <= startPos {
			return 0
		}
		// Clamp to prevent source [p, p+n) overlapping destination [startPos, startPos+n)
		return min(n, p-startPos)
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

	blocksize := blocksizeOfFile(f.Name())

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
