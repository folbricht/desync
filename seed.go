package desync

import (
	"os"
)

// Filesystem block size
const DefaultBlockSize = 4096

// Seed represent a source of chunks other than the store. Typically a seed is
// another index+blob that present on disk already and is used to copy or clone
// existing chunks or blocks into the target from.
type Seed interface {
	LongestMatchWith(chunks []IndexChunk) (int, SeedSegment)
}

// SeedSegment represents a matching range between a Seed and a a file being
// assembled from an Index. It's used to copy or reflink data from seeds into
// a target file during an extract operation.
type SeedSegment interface {
	Size() uint64
	WriteInto(dst *os.File, offset, end, blocksize uint64, isBlank bool) error
}

// indexSegment represents a contiguous section of an index which is used when
// assembling a file from seeds. first/last are positions in the index.
type indexSegment struct {
	index       Index
	first, last int
}

func (s indexSegment) lengthChunks() int {
	return s.last - s.first + 1
}

func (s indexSegment) lengthBytes() uint64 {
	return s.end() - s.start()
}

func (s indexSegment) start() uint64 {
	return s.index.Chunks[s.first].Start
}

func (s indexSegment) end() uint64 {
	return s.index.Chunks[s.last].Start + s.index.Chunks[s.last].Size
}

func (s indexSegment) chunks() []IndexChunk {
	return s.index.Chunks[s.first : s.last+1]
}
