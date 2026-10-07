package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// DefaultBlockSize is used when the actual filesystem block size cannot be determined automatically
const DefaultBlockSize = 4096

// cloneRange is an indirection over CloneRange that allows tests to simulate
// filesystems where CanClone succeeds but actual cloning fails (e.g. ZFS).
var cloneRange = CloneRange

// Seed represent a source of chunks other than the store. Typically a seed is
// another index+blob that present on disk already and is used to copy or clone
// existing chunks or blocks into the target from.
type Seed interface {
	// LongestMatchFrom returns the longest sequence of chunks anywhere in the
	// seed that match chunks starting at chunks[startPos]. It returns the
	// chunk position of the match in the seed and the number of matching
	// chunks, or (0, 0) if there is no match.
	LongestMatchFrom(chunks []IndexChunk, startPos int) (pos int, n int)
	// GetSegment returns a SeedSegment covering n chunks starting at chunk
	// position pos in the seed. pos must be a position returned by
	// LongestMatchFrom; n may be smaller than the reported match length.
	GetSegment(pos, n int) SeedSegment
	RegenerateIndex(ctx context.Context, n int, attempt int, seedNumber int) error
}

// seedIndex holds a seed's index along with a lookup table of chunk positions
// by ID. It implements the matching kernel shared by file seeds and the
// self-seed.
type seedIndex struct {
	index      Index
	pos        map[ChunkID][]int
	canReflink bool
}

func newSeedIndex(index Index, canReflink bool) seedIndex {
	s := seedIndex{
		index:      index,
		pos:        make(map[ChunkID][]int, len(index.Chunks)),
		canReflink: canReflink,
	}
	for i, c := range index.Chunks {
		s.pos[c.ID] = append(s.pos[c.ID], i)
	}
	return s
}

// longestMatchFrom finds, among the seed positions holding the same chunk as
// chunks[startPos], the position with the longest run of consecutive matches.
// Only positions from minPos on are candidates; they're found by binary
// search, so a chunk repeated many times before minPos costs nothing. clamp,
// if not nil, returns the maximum run length allowed for a candidate seed
// position; returning 0 discards the candidate. maxCandidates, when not zero,
// bounds how many candidate positions are examined. Later candidates win ties.
// Returns the winning position and run length, or (0, 0) if there is no match.
func (s *seedIndex) longestMatchFrom(chunks []IndexChunk, startPos, minPos, maxCandidates int, clamp func(p int) int) (int, int) {
	if startPos >= len(chunks) || len(s.index.Chunks) == 0 {
		return 0, 0
	}
	pos, ok := s.pos[chunks[startPos].ID]
	if !ok {
		return 0, 0
	}
	pos = pos[sort.SearchInts(pos, minPos):]
	var (
		bestPos  int
		maxLen   int
		limit    int
		examined int
	)
	if !s.canReflink {
		// Limit the maximum number of chunks in a single sequence to avoid
		// having jobs that are too unbalanced. However, if reflinks are
		// supported, we don't limit it to make it faster and take less space.
		limit = 100
	}
	for _, p := range pos {
		// Applying the clamp as a match limit also bounds the work done per
		// candidate.
		lim := limit
		if clamp != nil {
			c := clamp(p)
			if c == 0 {
				continue
			}
			if lim == 0 || c < lim {
				lim = c
			}
		}
		n := maxMatchFrom(chunks[startPos:], s.index.Chunks, p, lim)
		if n > 0 && n >= maxLen {
			bestPos = p
			maxLen = n
		}
		if limit != 0 && limit == maxLen {
			break
		}
		examined++
		if maxCandidates != 0 && examined == maxCandidates {
			break
		}
	}
	return bestPos, maxLen
}

func (s *seedIndex) mayClone(isBlank bool) bool { return s.canReflink }

// clones reports whether the chunks from position pos in the seed would be
// cloned rather than copied over the target chunks dst.
func (s *seedIndex) clones(pos int, dst []IndexChunk, blocksize uint64, isBlank bool) bool {
	return s.canReflink && s.reflinkable(pos, dst, blocksize)
}

// reflinkable reports whether the chunks from position pos in the seed would
// be cloned rather than copied over the target chunks dst, if the seed
// supports reflinks.
func (s *seedIndex) reflinkable(pos int, dst []IndexChunk, blocksize uint64) bool {
	return reflinkable(s.index.Chunks[pos].Start, chunkRangeLength(dst), dst[0].Start, blocksize)
}

// maxMatchFrom compares chunks starting at position 0 with seedChunks starting
// at position p. Returns the number of consecutive matching chunks. A limit of
// zero means no limit.
func maxMatchFrom(chunks, seedChunks []IndexChunk, p, limit int) int {
	if len(chunks) == 0 {
		return 0
	}
	var (
		sp int
		dp = p
	)
	for {
		if limit != 0 && sp == limit {
			break
		}
		if dp >= len(seedChunks) || sp >= len(chunks) {
			break
		}
		if chunks[sp].ID != seedChunks[dp].ID {
			break
		}
		dp++
		sp++
	}
	return dp - p
}

// SeedSegment represents a matching range between a Seed and a file being
// assembled from an Index. It's used to copy or reflink data from seeds into
// a target file during an extract operation.
type SeedSegment interface {
	FileName() string
	Validate(file *os.File) error
	WriteInto(dst *os.File, offset, end, blocksize uint64, isBlank bool) (copied uint64, cloned uint64, err error)
}

// chunkBufPool holds read buffers for chunkInPlace callers that don't bring
// their own, so validating seeds doesn't allocate a buffer per chunk.
var chunkBufPool sync.Pool

// getChunkBuf returns a buffer of at least size bytes from the pool. It has
// to be returned to the pool once it's no longer used.
func getChunkBuf(size uint64) *[]byte {
	p, _ := chunkBufPool.Get().(*[]byte)
	if p == nil || uint64(len(*p)) < size {
		b := make([]byte, size)
		p = &b
	}
	return p
}

// chunkInPlace reports whether f already contains the chunk's data at the
// chunk's position. buf is used for reading when it is large enough, which
// allows callers checking many chunks to reuse one allocation. If it's nil,
// a buffer from a pool is used.
func chunkInPlace(f *os.File, c IndexChunk, buf []byte) bool {
	if uint64(len(buf)) < c.Size {
		p := getChunkBuf(c.Size)
		defer chunkBufPool.Put(p)
		buf = *p
	}
	b := buf[:c.Size]
	if _, err := f.ReadAt(b, int64(c.Start)); err != nil {
		return false
	}
	return Digest.Sum(b) == c.ID
}

// maxChunkSize is the size of the largest chunk, the buffer a reader of all of
// them needs.
func maxChunkSize(chunks []IndexChunk) uint64 {
	var size uint64
	for _, c := range chunks {
		size = max(size, c.Size)
	}
	return size
}

// chunksInPlace reports whether f contains the data of every chunk at the
// chunk's position, reading them all into one buffer.
func chunksInPlace(f *os.File, chunks []IndexChunk) bool {
	p := getChunkBuf(maxChunkSize(chunks))
	defer chunkBufPool.Put(p)
	for _, c := range chunks {
		if !chunkInPlace(f, c, *p) {
			return false
		}
	}
	return true
}

// copyRange copies length bytes from src at srcOffset to dst at dstOffset.
// src and dst must be distinct file handles since both handles' offsets are
// moved, but they may refer to the same file as long as the ranges don't
// overlap.
func copyRange(dst, src *os.File, srcOffset, length, dstOffset uint64) (uint64, uint64, error) {
	if _, err := dst.Seek(int64(dstOffset), io.SeekStart); err != nil {
		return 0, 0, err
	}
	if _, err := src.Seek(int64(srcOffset), io.SeekStart); err != nil {
		return 0, 0, err
	}
	// Copy using a fixed buffer. Using io.Copy() with a LimitReader will make it
	// create a buffer matching N of the LimitReader which can be too large
	copied, err := io.CopyBuffer(dst, io.LimitReader(src, int64(length)), make([]byte, 64*1024))
	return uint64(copied), 0, err
}

// matchingSeed is the part of a seed the plan uses to find the chunks it can
// provide. The self-seed implements it without being a full Seed, its
// segments need the target offset they're written to.
type matchingSeed interface {
	LongestMatchFrom(chunks []IndexChunk, startPos int) (pos int, n int)
}

// cloningSeed is implemented by seeds that can tell whether a match would be
// cloned into the target, or not need to be written at all, rather than be
// copied. Seeds that don't implement it are assumed to copy.
type cloningSeed interface {
	// mayClone reports whether the seed can clone any chunks at all. Looking
	// for matches to clone is skipped for seeds that can't.
	mayClone(isBlank bool) bool
	// clones reports whether the seed's chunks from chunk position pos would
	// be cloned when written over the target chunks dst.
	clones(pos int, dst []IndexChunk, blocksize uint64, isBlank bool) bool
}

// reflinkable reports whether cloneOrCopyRange clones at least one block of
// a range rather than only copying it. The ranges need the same alignment
// relative to blocksize and have to span a full aligned block.
func reflinkable(srcOffset, length, dstOffset, blocksize uint64) bool {
	if srcOffset%blocksize != dstOffset%blocksize {
		return false
	}
	srcAlignStart := (srcOffset/blocksize + 1) * blocksize
	srcAlignEnd := (srcOffset + length) / blocksize * blocksize
	return srcAlignEnd > srcAlignStart
}

// cloneOrCopyRange reflinks the aligned blocks the two ranges have in common
// and copies the unaligned head and tail. Both ranges must have the same
// alignment relative to blocksize. Falls back to copying whenever cloning
// isn't possible.
func cloneOrCopyRange(dst, src *os.File, srcOffset, length, dstOffset, blocksize uint64) (uint64, uint64, error) {
	if srcOffset%blocksize != dstOffset%blocksize {
		return 0, 0, fmt.Errorf("reflink ranges not aligned between %s and %s", src.Name(), dst.Name())
	}

	// If the range is too small to contain a full aligned block, there is
	// nothing that can be cloned. Copy the whole range instead. This also
	// guards against srcAlignEnd-srcAlignStart underflowing, and against
	// calling CloneRange with a zero length, which FICLONERANGE interprets
	// as "clone to the end of the source file". Filesystems with large
	// blocks, like ZFS with the default 128k recordsize, hit this case
	// frequently.
	if !reflinkable(srcOffset, length, dstOffset, blocksize) {
		return copyRange(dst, src, srcOffset, length, dstOffset)
	}
	srcAlignStart := (srcOffset/blocksize + 1) * blocksize
	srcAlignEnd := (srcOffset + length) / blocksize * blocksize

	dstAlignStart := (dstOffset/blocksize + 1) * blocksize
	alignLength := srcAlignEnd - srcAlignStart
	dstAlignEnd := dstAlignStart + alignLength

	// fill the area before the first aligned block
	var copied uint64
	c1, _, err := copyRange(dst, src, srcOffset, srcAlignStart-srcOffset, dstOffset)
	if err != nil {
		return c1, 0, err
	}
	copied += c1
	// fill the area after the last aligned block
	c2, _, err := copyRange(dst, src, srcAlignEnd, srcOffset+length-srcAlignEnd, dstAlignEnd)
	if err != nil {
		return copied + c2, 0, err
	}
	copied += c2
	// close the aligned blocks
	if err := cloneRange(dst, src, srcAlignStart, alignLength, dstAlignStart); err != nil {
		// Not every filesystem that passes the CanClone probe can clone every
		// range. ZFS for example requires alignment to its record size and
		// refuses to clone data that hasn't been committed to disk yet. Fall
		// back to copying the blocks.
		c3, _, err := copyRange(dst, src, srcAlignStart, alignLength, dstAlignStart)
		return copied + c3, 0, err
	}
	return copied, alignLength, nil
}
