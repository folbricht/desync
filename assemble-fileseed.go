package desync

import (
	"context"
	"fmt"
	"os"
)

// FileSeed is used to copy or clone blocks from an existing index+blob during
// file extraction.
type FileSeed struct {
	srcFile string
	seedIndex
	canReflink bool
}

// NewFileSeed initializes a new seed that uses an existing index and its blob
func NewFileSeed(dstFile string, srcFile string, index Index) (*FileSeed, error) {
	return &FileSeed{
		srcFile:    srcFile,
		seedIndex:  newSeedIndex(index),
		canReflink: CanClone(dstFile, srcFile),
	}, nil
}

// LongestMatchFrom returns the longest sequence of chunks anywhere in the seed
// that match chunks starting at chunks[startPos]. It returns the chunk
// position of the match in the seed and the number of matching chunks, or
// (0, 0) if there is no match.
func (s *FileSeed) LongestMatchFrom(chunks []IndexChunk, startPos int) (int, int) {
	return s.longestMatchFrom(chunks, startPos, s.canReflink, 0, nil)
}

// GetSegment constructs a SeedSegment for n chunks starting at chunk position
// pos in the seed.
func (s *FileSeed) GetSegment(pos, n int) SeedSegment {
	return newFileSeedSegment(s.srcFile, s.index.Chunks[pos:pos+n], s.canReflink)
}

func (s *FileSeed) RegenerateIndex(ctx context.Context, n int, attempt int, seedNumber int) error {
	chunkingPrefix := fmt.Sprintf("Attempt %d: Chunking Seed %d ", attempt, seedNumber)
	index, _, err := IndexFromFile(ctx, s.srcFile, n, s.index.Index.ChunkSizeMin, s.index.Index.ChunkSizeAvg,
		s.index.Index.ChunkSizeMax, NewProgressBar(chunkingPrefix))
	if err != nil {
		return err
	}

	s.seedIndex = newSeedIndex(index)
	return nil
}

type fileSeedSegment struct {
	file       string
	chunks     []IndexChunk
	canReflink bool
}

func newFileSeedSegment(file string, chunks []IndexChunk, canReflink bool) *fileSeedSegment {
	return &fileSeedSegment{
		canReflink: canReflink,
		file:       file,
		chunks:     chunks,
	}
}

func (s *fileSeedSegment) FileName() string {
	return s.file
}

func (s *fileSeedSegment) Size() uint64 {
	return chunkRangeLength(s.chunks)
}

func (s *fileSeedSegment) WriteInto(dst *os.File, offset, length, blocksize uint64, isBlank bool) (uint64, uint64, error) {
	if length != s.Size() {
		return 0, 0, fmt.Errorf("unable to copy %d bytes from %s to %s : wrong size", length, s.file, dst.Name())
	}
	src, err := os.Open(s.file)
	if err != nil {
		return 0, 0, err
	}
	defer src.Close()

	// Do a straight copy if reflinks are not supported or blocks aren't aligned
	if !s.canReflink || s.chunks[0].Start%blocksize != offset%blocksize {
		return copyRange(dst, src, s.chunks[0].Start, length, offset)
	}
	return cloneOrCopyRange(dst, src, s.chunks[0].Start, length, offset, blocksize)
}

// Validate compares all chunks in this slice of the seed index to the underlying data
// and fails if they don't match.
func (s *fileSeedSegment) Validate(file *os.File) error {
	for _, c := range s.chunks {
		if !chunkInPlace(file, c, nil) {
			return fmt.Errorf("seed index for %s doesn't match its data", s.file)
		}
	}
	return nil
}

type fileSeedSource struct {
	segment   SeedSegment
	seed      Seed
	offset    uint64
	length    uint64
	blocksize uint64
	isBlank   bool
}

func (s *fileSeedSource) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	return s.segment.WriteInto(f, s.offset, s.length, s.blocksize, s.isBlank)
}

func (s *fileSeedSource) recordStats(stats *ExtractStats, numChunks int) {
	stats.addChunksFromSeed(uint64(numChunks))
}

func (s *fileSeedSource) Seed() Seed   { return s.seed }
func (s *fileSeedSource) File() string { return s.segment.FileName() }

// needsValidation reports whether the source is backed by a seed file whose
// content must be checked against the index. Null-chunk segments have no
// backing file and are always valid.
func (s *fileSeedSource) needsValidation() bool { return s.segment.FileName() != "" }

func (s *fileSeedSource) Validate(file *os.File) error {
	return s.segment.Validate(file)
}

func (s *fileSeedSource) String() string {
	return fmt.Sprintf("FileSeed(%s): Copy to [%d:%d]", s.segment.FileName(), s.offset, s.offset+s.length)
}
