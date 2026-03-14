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

// longestMatchFrom returns the longest sequence of matching chunks after a
// given starting position.
func (s *selfSeed) longestMatchFrom(chunks []IndexChunk, startPos int) (int, int) {
	if len(chunks) <= startPos || len(s.index.Chunks) == 0 {
		return 0, 0
	}
	pos, ok := s.pos[chunks[startPos].ID]
	if !ok {
		return 0, 0
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
		start, n := s.maxMatchFrom(chunks[startPos:], p, limit)
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
	return maxStart, maxLen
}

func (s *selfSeed) maxMatchFrom(chunks []IndexChunk, p int, limit int) (int, int) {
	if len(chunks) == 0 {
		return 0, 0
	}
	var (
		sp int
		dp = p
	)
	for {
		if limit != 0 && sp == limit {
			break
		}
		if dp >= len(s.index.Chunks) || sp >= len(chunks) {
			break
		}
		if chunks[sp].ID != s.index.Chunks[dp].ID {
			break
		}
		dp++
		sp++
	}
	return p, dp - p
}

func (s *selfSeed) getSegment(from, to, length int) *selfSeedSegment {
	return &selfSeedSegment{
		seed:   s,
		from:   from,
		to:     to,
		length: length,
	}
}

type selfSeedSegment struct {
	seed   *selfSeed
	from   int // Index of the first chunk to copy from
	to     int // Index of the first chunk to copy to
	length int // Number of chunks to copy
}

func (s *selfSeedSegment) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	srcStart := s.seed.index.Chunks[s.from].Start
	dstStart := s.seed.index.Chunks[s.to].Start
	lastFrom := s.from + s.length - 1
	length := s.seed.index.Chunks[lastFrom].Start + s.seed.index.Chunks[lastFrom].Size - srcStart

	blocksize := blocksizeOfFile(f.Name())

	// Use reflinks if supported and blocks are aligned
	if s.seed.canReflink && srcStart%blocksize == dstStart%blocksize {
		return 0, length, CloneRange(f, f, srcStart, length, dstStart)
	}

	// Borrow a read handle from the pool
	src := <-s.seed.readers
	defer func() { s.seed.readers <- src }()

	if _, err := src.Seek(int64(srcStart), io.SeekStart); err != nil {
		return 0, 0, err
	}
	if _, err := f.Seek(int64(dstStart), io.SeekStart); err != nil {
		return 0, 0, err
	}
	_, err = io.CopyBuffer(f, io.LimitReader(src, int64(length)), make([]byte, 64*1024))
	return length, 0, err
}

func (s *selfSeedSegment) String() string {
	fromStart := s.seed.index.Chunks[s.from].Start
	toStart := s.seed.index.Chunks[s.to].Start
	lastFromChunkIndex := s.from + s.length - 1
	lastToChunkIndex := s.to + s.length - 1
	fromEnd := s.seed.index.Chunks[lastFromChunkIndex].Start + s.seed.index.Chunks[lastFromChunkIndex].Size
	toEnd := s.seed.index.Chunks[lastToChunkIndex].Start + s.seed.index.Chunks[lastToChunkIndex].Size

	return fmt.Sprintf("SelfSeed: Copy [%d:%d] to [%d:%d]", fromStart, fromEnd, toStart, toEnd)
}
