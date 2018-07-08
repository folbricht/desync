package desync

import (
	"crypto/sha512"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Filesystem block size
const BlockSize = 4096

// Seed represent a source of chunks other than the store. Typically a seed is
// another index+blob that present on disk already and is used to copy or clone
// existing chunks or blocks into the target from.
type Seed interface {
	longestMatchWith(chunks []IndexChunk) (int, section)
}

type section interface {
	size() uint64
	writeInto(dst *os.File, start, end uint64) error
}

// IndexSeed is used to copy or clone blocks from an existing index+blob during
// file extraction.
type IndexSeed struct {
	srcFile    string
	index      Index
	pos        map[ChunkID][]int
	canReflink bool
	blocksize  int
}

type fileSection struct {
	seed   *IndexSeed
	chunks []IndexChunk
}

func (f *fileSection) start() uint64 {
	if len(f.chunks) == 0 {
		return 0
	}
	return f.chunks[0].Start
}

func (f *fileSection) end() uint64 {
	if len(f.chunks) == 0 {
		return 0
	}
	last := f.chunks[len(f.chunks)-1]
	return last.Start + last.Size
}

func (f *fileSection) size() uint64 {
	return f.end() - f.start()
}

func (s *fileSection) writeInto(dst *os.File, start, end uint64) error {
	if end-start != s.size() {
		return fmt.Errorf("unable to copy %d bytes from %s to %s : wrong size", end-start, s.seed.srcFile, dst.Name())
	}
	src, err := os.Open(s.seed.srcFile)
	if err != nil {
		return err
	}
	defer src.Close()

	// Make sure the data we're planning on pulling from the file matches what
	// the index says it is.
	if err := s.validate(src); err != nil {
		return err
	}

	// Do a straight copy if reflinks are not supported
	if !s.seed.canReflink {
		return s.copy(dst, src, s.chunks[0].Start, end-start, start)
	}
	return s.clone(dst, src, s.chunks[0].Start, end-start, start)
}

// Compares all chunks in this slice of the seed index to the underlying data
// and fails if they don't match.
func (s *fileSection) validate(src *os.File) error {
	for _, c := range s.chunks {
		b := make([]byte, c.Size)
		if _, err := src.ReadAt(b, int64(c.Start)); err != nil {
			return err
		}
		sum := sha512.Sum512_256(b)
		if sum != c.ID {
			return fmt.Errorf("seed index for %s doesn't match its data", s.seed.srcFile)
		}
	}
	return nil
}

// Performs a plain copy of everything in the seed to the target, not cloning
// of blocks.
func (s *fileSection) copy(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
	if _, err := dst.Seek(int64(dstOffset), os.SEEK_SET); err != nil {
		return err
	}
	if _, err := src.Seek(int64(srcOffset), os.SEEK_SET); err != nil {
		return err
	}
	_, err := io.Copy(dst, io.LimitReader(src, int64(srcLength)))
	return err
}

// Reflink the overlapping blocks in the two ranges and copy the bit before and
// after the blocks.
func (s *fileSection) clone(dst, src *os.File, srcOffset, srcLength, dstOffset uint64) error {
	if srcOffset%uint64(s.seed.blocksize) != dstOffset%uint64(s.seed.blocksize) {
		return fmt.Errorf("reflink ranges not aligned between %s and %s", src.Name(), dst.Name())
	}

	srcAlignStart := (srcOffset/uint64(s.seed.blocksize) + 1) * uint64(s.seed.blocksize)
	srcAlignEnd := (srcOffset + srcLength) / uint64(s.seed.blocksize) * uint64(s.seed.blocksize)
	dstAlignStart := (dstOffset/uint64(s.seed.blocksize) + 1) * uint64(s.seed.blocksize)
	alignLength := srcAlignEnd - srcAlignStart
	dstAlignEnd := dstAlignStart + alignLength

	// fill the area before the first aligned block
	if err := s.copy(dst, src, srcOffset, srcAlignStart-srcOffset, dstOffset); err != nil {
		return err
	}
	// fill the area after the last aligned block
	if err := s.copy(dst, src, srcAlignEnd, srcOffset+srcLength-srcAlignEnd, dstAlignEnd); err != nil {
		return err
	}
	// close the aligned blocks
	return CloneRange(dst, src, srcAlignStart, alignLength, dstAlignStart)
}

// NewIndexSeed initializes a new seed that uses an existing index and its blob
func NewIndexSeed(dstFile string, blocksize int, srcFile string, index Index) (*IndexSeed, error) {
	s := IndexSeed{
		srcFile:    srcFile,
		pos:        make(map[ChunkID][]int),
		index:      index,
		canReflink: CanClone(dstFile, srcFile),
		blocksize:  blocksize,
	}
	for i, c := range s.index.Chunks {
		s.pos[c.ID] = append(s.pos[c.ID], i)
	}
	return &s, nil
}

// longestMatchWith returns the longest sequence of of chunks anywhere in Source
// that match b starting at b[0]. If there is no match, it returns nil
func (s *IndexSeed) longestMatchWith(chunks []IndexChunk) (int, section) {
	if len(chunks) == 0 || len(s.index.Chunks) == 0 {
		return 0, nil
	}
	pos, ok := s.pos[chunks[0].ID]
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
	return max, &fileSection{seed: s, chunks: match}
}

// Returns a slice of chunks from the seed. Compares chunks from position 0
// with seed chunks starting at p.
func (s *IndexSeed) maxMatchFrom(chunks []IndexChunk, p int) []IndexChunk {
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

type nullChunkSeed struct {
	id         ChunkID
	blocksize  int
	blockfile  *os.File
	canReflink bool
}

func newNullChunkSeed(dstFile string, blocksize int, max uint64) (*nullChunkSeed, error) {
	blockfile, err := ioutil.TempFile(filepath.Dir(dstFile), ".tmp-block")
	if err != nil {
		return nil, err
	}
	var canReflink bool
	if CanClone(dstFile, blockfile.Name()) {
		canReflink = true
		b := make([]byte, blocksize)
		if _, err := blockfile.Write(b); err != nil {
			return nil, err
		}
	}
	return &nullChunkSeed{
		id:         NewNullChunk(max).ID,
		canReflink: canReflink,
		blockfile:  blockfile,
		blocksize:  blocksize,
	}, nil
}

func (s *nullChunkSeed) close() error {
	if s.blockfile != nil {
		s.blockfile.Close()
		return os.Remove(s.blockfile.Name())
	}
	return nil
}

func (s *nullChunkSeed) longestMatchWith(chunks []IndexChunk) (int, section) {
	if len(chunks) == 0 {
		return 0, nil
	}
	var n int
	for _, c := range chunks {
		if c.ID != s.id {
			break
		}
		n++
	}
	if n == 0 {
		return 0, nil
	}
	return n, &nullChunkSection{
		from:       chunks[0].Start,
		to:         chunks[n-1].Start + chunks[n-1].Size,
		blocksize:  s.blocksize,
		blockfile:  s.blockfile,
		canReflink: s.canReflink,
	}
}

type nullChunkSection struct {
	from, to   uint64
	blocksize  int
	blockfile  *os.File
	canReflink bool
}

func (s *nullChunkSection) start() uint64 { return s.from }

func (s *nullChunkSection) end() uint64 { return s.to }

func (s *nullChunkSection) size() uint64 { return s.to - s.from }

func (s *nullChunkSection) writeInto(dst *os.File, start, end uint64) error {
	if end-start != s.size() {
		return fmt.Errorf("unable to copy %d bytes to %s : wrong size", end-start, dst.Name())
	}

	if !s.canReflink {
		return s.copy(dst, start, s.size())
	}
	return s.clone(dst, start, s.size())
}

func (s *nullChunkSection) copy(dst *os.File, dstOffset, length uint64) error {
	if _, err := dst.Seek(int64(dstOffset), os.SEEK_SET); err != nil {
		return err
	}
	_, err := io.Copy(dst, io.LimitReader(nullReader{}, int64(length)))
	return err
}

func (s *nullChunkSection) clone(dst *os.File, dstOffset, length uint64) error {
	dstAlignStart := (dstOffset/uint64(s.blocksize) + 1) * uint64(s.blocksize)
	dstAlignEnd := (dstOffset + length) / uint64(s.blocksize) * uint64(s.blocksize)
	// alignLength := dstAlignEnd - dstAlignStart

	// fill the area before the first aligned block
	if err := s.copy(dst, dstOffset, dstAlignStart-dstOffset); err != nil {
		return err
	}
	// fill the area after the last aligned block
	if err := s.copy(dst, dstAlignEnd, dstOffset+length-dstAlignEnd); err != nil {
		return err
	}
	for offset := dstAlignStart; offset < dstAlignEnd; offset += uint64(s.blocksize) {
		if err := CloneRange(dst, s.blockfile, 0, uint64(s.blocksize), offset); err != nil {
			return err
		}
	}
	return nil
}

type nullReader struct{}

func (r nullReader) Read(b []byte) (n int, err error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}
