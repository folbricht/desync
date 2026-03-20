package desync

import (
	"fmt"
	"io"
	"os"
)

type selfSeed struct {
	file       string
	index      Index
	pos        map[ChunkID][]int
	canReflink bool
	readers    chan *os.File
}

func newSelfSeed(file string, index Index, n int) (*selfSeed, error) {
	s := &selfSeed{
		file:       file,
		pos:        make(map[ChunkID][]int),
		index:      index,
		canReflink: CanClone(file, file),
		readers:    make(chan *os.File, n),
	}
	for i, c := range s.index.Chunks {
		s.pos[c.ID] = append(s.pos[c.ID], i)
	}
	// Only open read handles if the file exists. If it doesn't, self-seed
	// segments won't be created since there's nothing to match.
	if _, err := os.Stat(file); err == nil {
		for range n {
			f, err := os.Open(file)
			if err != nil {
				s.Close()
				return nil, err
			}
			s.readers <- f
		}
	}
	return s, nil
}

func (s *selfSeed) Close() {
	if s.readers == nil {
		return
	}
	close(s.readers)
	for f := range s.readers {
		f.Close()
	}
	s.readers = nil
}

// LongestMatchFrom returns the longest sequence of matching chunks after a
// given starting position.
func (s *selfSeed) LongestMatchFrom(chunks []IndexChunk, startPos int) (uint64, uint64, int, int) {
	if len(chunks) <= startPos || len(s.index.Chunks) == 0 {
		return 0, 0, 0, 0
	}
	pos, ok := s.pos[chunks[startPos].ID]
	if !ok {
		return 0, 0, 0, 0
	}
	// From every position of chunks[startPos] in the source, find a slice of
	// matching chunks. Then return the longest of those slices.
	var (
		maxStart int
		maxLen   int
		limit    int
	)
	if !s.canReflink {
		// Limit the maximum number of chunks, in a single sequence, to
		// avoid having jobs that are too unbalanced. However, if
		// reflinks are supported, we don't limit it to make it faster
		// and take less space.
		limit = 100
	}
	for _, p := range pos {
		if p <= startPos {
			continue
		}
		start, n := maxMatchFrom(chunks[startPos:], s.index.Chunks, p, limit)
		// Clamp to prevent source [p, p+n) overlapping destination [startPos, startPos+n)
		if max := p - startPos; n > max {
			n = max
		}
		if n >= maxLen { // Using >= here to get the last (longest) match
			maxStart = start
			maxLen = n
		}
		if limit != 0 && limit == maxLen {
			break
		}
	}
	if maxLen == 0 {
		return 0, 0, 0, 0
	}
	byteOffset := s.index.Chunks[maxStart].Start
	last := s.index.Chunks[maxStart+maxLen-1]
	byteLength := last.Start + last.Size - byteOffset
	return byteOffset, byteLength, maxStart, maxLen
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
	blocksize := blocksizeOfFile(f.Name())

	// Use reflinks if supported and blocks are aligned
	if s.seed.canReflink && s.srcOffset%blocksize == s.dstOffset%blocksize {
		return 0, s.size, CloneRange(f, f, s.srcOffset, s.size, s.dstOffset)
	}

	// Borrow a read handle from the pool
	src := <-s.seed.readers
	defer func() { s.seed.readers <- src }()

	if _, err := src.Seek(int64(s.srcOffset), io.SeekStart); err != nil {
		return 0, 0, err
	}
	if _, err := f.Seek(int64(s.dstOffset), io.SeekStart); err != nil {
		return 0, 0, err
	}
	_, err = io.CopyBuffer(f, io.LimitReader(src, int64(s.size)), make([]byte, 64*1024))
	return s.size, 0, err
}

func (s *selfSeedSegment) String() string {
	return fmt.Sprintf("SelfSeed: Copy [%d:%d] to [%d:%d]",
		s.srcOffset, s.srcOffset+s.size,
		s.dstOffset, s.dstOffset+s.size)
}
