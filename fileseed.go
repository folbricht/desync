package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// FileSeed is used to copy or clone blocks from an existing index+blob during
// file extraction.
type FileSeed struct {
	srcFile    string
	index      Index
	pos        map[ChunkID][]int
	canReflink bool
	isInvalid  bool
	mu         sync.RWMutex
}

// NewIndexSeed initializes a new seed that uses an existing index and its blob
func NewIndexSeed(dstFile string, srcFile string, index Index) (*FileSeed, error) {
	s := FileSeed{
		srcFile:    srcFile,
		pos:        make(map[ChunkID][]int),
		index:      index,
		canReflink: CanClone(dstFile, srcFile),
		isInvalid:  false,
	}
	for i, c := range s.index.Chunks {
		s.pos[c.ID] = append(s.pos[c.ID], i)
	}
	return &s, nil
}

// LongestMatchWith returns the longest sequence of chunks anywhere in Source
// that match `chunks` starting at chunks[0]. If there is no match, it returns a
// length of zero and a nil SeedSegment.
func (s *FileSeed) LongestMatchWith(chunks []IndexChunk) (int, SeedSegment) {
	s.mu.RLock()
	// isInvalid can be concurrently read or wrote. Use a mutex to avoid a race
	if len(chunks) == 0 || len(s.index.Chunks) == 0 || s.isInvalid {
		return 0, nil
	}
	s.mu.RUnlock()
	pos, ok := s.pos[chunks[0].ID]
	if !ok {
		return 0, nil
	}
	// From every position of chunks[0] in the source, find a slice of
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
	return max, newFileSeedSegment(s.srcFile, match, s.canReflink)
}

func (s *FileSeed) RegenerateIndex(ctx context.Context, n int, attempt int, seedNumber int) error {
	chunkingPrefix := fmt.Sprintf("Attempt %d: Chunking Seed %d ", attempt, seedNumber)
	index, _, err := IndexFromFile(ctx, s.srcFile, n, s.index.Index.ChunkSizeMin, s.index.Index.ChunkSizeAvg,
		s.index.Index.ChunkSizeMax, NewProgressBar(chunkingPrefix))
	if err != nil {
		return err
	}

	s.index = index
	s.SetInvalid(false)
	s.pos = make(map[ChunkID][]int, len(s.index.Chunks))
	for i, c := range s.index.Chunks {
		s.pos[c.ID] = append(s.pos[c.ID], i)
	}

	return nil
}

func (s *FileSeed) SetInvalid(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isInvalid = value
}

func (s *FileSeed) IsInvalid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isInvalid
}

// Returns a slice of chunks from the seed. Compares chunks from position 0
// with seed chunks starting at p.
func (s *FileSeed) maxMatchFrom(chunks []IndexChunk, p int) []IndexChunk {
	if len(chunks) == 0 {
		return nil
	}
	var (
		sp int
		dp = p
	)
	for {
		if dp >= len(s.index.Chunks) || sp >= len(chunks) {
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

type fileSeedSegment struct {
	file           string
	chunks         []IndexChunk
	canReflink     bool
	needValidation bool
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
	if len(s.chunks) == 0 {
		return 0
	}
	last := s.chunks[len(s.chunks)-1]
	return last.Start + last.Size - s.chunks[0].Start
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
		return s.copy(dst, src, s.chunks[0].Start, length, offset)
	}
	return s.clone(dst, src, s.chunks[0].Start, length, offset, blocksize)
}

// Validate compares all chunks in this slice of the seed index to the underlying data
// and fails if they don't match.
func (s *fileSeedSegment) Validate(file *os.File) error {
	for _, c := range s.chunks {
		b := make([]byte, c.Size)
		if _, err := file.ReadAt(b, int64(c.Start)); err != nil {
			return err
		}
		sum := Digest.Sum(b)
		if sum != c.ID {
			return fmt.Errorf("seed index for %s doesn't match its data", s.file)
		}
	}
	return nil
}

// Performs a plain copy of everything in the seed to the target, not cloning
// of blocks.
func (s *fileSeedSegment) copy(dst, src *os.File, srcOffset, length, dstOffset uint64) (uint64, uint64, error) {
	if _, err := dst.Seek(int64(dstOffset), os.SEEK_SET); err != nil {
		return 0, 0, err
	}
	if _, err := src.Seek(int64(srcOffset), os.SEEK_SET); err != nil {
		return 0, 0, err
	}

	// Copy using a fixed buffer. Using io.Copy() with a LimitReader will make it
	// create a buffer matching N of the LimitReader which can be too large
	copied, err := io.CopyBuffer(dst, io.LimitReader(src, int64(length)), make([]byte, 64*1024))
	return uint64(copied), 0, err
}

// Reflink the overlapping blocks in the two ranges and copy the bit before and
// after the blocks.
func (s *fileSeedSegment) clone(dst, src *os.File, srcOffset, srcLength, dstOffset, blocksize uint64) (uint64, uint64, error) {
	if srcOffset%blocksize != dstOffset%blocksize {
		return 0, 0, fmt.Errorf("reflink ranges not aligned between %s and %s", src.Name(), dst.Name())
	}

	srcAlignStart := (srcOffset/blocksize + 1) * blocksize
	srcAlignEnd := (srcOffset + srcLength) / blocksize * blocksize
	dstAlignStart := (dstOffset/blocksize + 1) * blocksize
	alignLength := srcAlignEnd - srcAlignStart
	dstAlignEnd := dstAlignStart + alignLength

	// fill the area before the first aligned block
	var copied uint64
	c1, _, err := s.copy(dst, src, srcOffset, srcAlignStart-srcOffset, dstOffset)
	if err != nil {
		return c1, 0, err
	}
	copied += c1
	// fill the area after the last aligned block
	c2, _, err := s.copy(dst, src, srcAlignEnd, srcOffset+srcLength-srcAlignEnd, dstAlignEnd)
	if err != nil {
		return copied + c2, 0, err
	}
	copied += c2
	// close the aligned blocks
	return copied, alignLength, CloneRange(dst, src, srcAlignStart, alignLength, dstAlignStart)
}
