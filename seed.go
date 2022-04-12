package desync

import (
	"context"
	"os"
)

// DefaultBlockSize is used when the actual filesystem block size cannot be determined automatically
const DefaultBlockSize = 4096

// Seed represent a source of chunks other than the store. Typically a seed is
// another index+blob that present on disk already and is used to copy or clone
// existing chunks or blocks into the target from.
type Seed interface {
	LongestMatchWith(chunks []IndexChunk) (int, SeedSegment)
	RegenerateIndex(ctx context.Context, n int, attempt int, seedNumber int) error
	SetInvalid(value bool)
	IsInvalid() bool
}

// SeedSegment represents a matching range between a Seed and a file being
// assembled from an Index. It's used to copy or reflink data from seeds into
// a target file during an extract operation.
type SeedSegment interface {
	FileName() string
	Size() uint64
	Validate(file *os.File) error
	WriteInto(dst *os.File, offset, end, blocksize uint64, isBlank bool) (copied uint64, cloned uint64, err error)
}

// IndexSegment represents a contiguous section of an index which is used when
// assembling a file from seeds. first/last are positions in the index.
type IndexSegment struct {
	index       Index
	first, last int
}

func (s IndexSegment) lengthChunks() int {
	return s.last - s.first + 1
}

func (s IndexSegment) lengthBytes() uint64 {
	return s.end() - s.start()
}

func (s IndexSegment) start() uint64 {
	return s.index.Chunks[s.first].Start
}

func (s IndexSegment) end() uint64 {
	return s.index.Chunks[s.last].Start + s.index.Chunks[s.last].Size
}

func (s IndexSegment) chunks() []IndexChunk {
	return s.index.Chunks[s.first : s.last+1]
}
