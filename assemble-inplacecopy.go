package desync

import (
	"fmt"
	"os"
)

// inPlaceCopy moves a chunk, or a run of consecutive chunks, from one position
// to another within the same file. It uses ReadAt/WriteAt (pread/pwrite) which
// are position-independent and safe for concurrent use on the same file handle.
type inPlaceCopy struct {
	// chunks are the source chunks in the file, in order and without gaps
	// between them. A move covers several of them when one shift moves them
	// all, in which case its source and destination don't overlap.
	chunks    []IndexChunk
	dstOffset uint64
	seed      Seed
	file      string

	// clone is set for moves whose blocks can be cloned rather than
	// copied: the file supports reflinks, the source and destination have
	// the same alignment and don't overlap, which the kernel requires within
	// one file. A clone keeps its data when the source is overwritten later,
	// so it takes the place of reading the source without changing when the
	// move has to run. Only runs of several chunks are cloned. Cloning chunk
	// by chunk locks and flushes the file for every one of them, which is
	// slower than copying and leaves the file in thousands of extents.
	clone     bool
	blocksize uint64

	// Moves that are part of a dependency cycle hold their source in a
	// buffer once other steps are about to overwrite it. If the buffer was
	// dropped for lack of memory, the chunk is taken from the store. Only
	// moves of a single chunk are buffered.
	buffer *inPlaceBuffer
	store  Store
}

// srcOffset is where the move reads its data.
func (s *inPlaceCopy) srcOffset() uint64 { return s.chunks[0].Start }

// size is how much data the move writes.
func (s *inPlaceCopy) size() uint64 { return chunkRangeLength(s.chunks) }

func (s *inPlaceCopy) Execute(f *os.File) (copied uint64, cloned uint64, err error) {
	// Write from the buffer, the source may have been overwritten since.
	if s.buffer != nil {
		data, err := s.buffer.take(f)
		if err != nil {
			return 0, 0, err
		}
		if data == nil {
			return s.fromStore().Execute(f)
		}
		if _, err := f.WriteAt(data, int64(s.dstOffset)); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy buffer write at %d: %w", s.dstOffset, err)
		}
		return s.size(), 0, nil
	}

	if s.clone {
		copied, cloned, err := s.cloneBlocks(f)
		if err == nil {
			return copied, cloned, nil
		}
		// Not every filesystem that passes the CanClone probe can clone every
		// range. Nothing was written yet, fall back to copying the data.
	}

	// Normal copy — read the source into a temp buffer, then write it to the
	// destination. Always buffer first to handle overlapping ranges safely.
	// One chunk at a time, so a move of a long run doesn't hold all of it in
	// memory; the ranges of a run don't overlap, its chunks can be moved in
	// any order.
	buf := getChunkBuf(maxChunkSize(s.chunks))
	defer chunkBufPool.Put(buf)
	offset := s.dstOffset
	for _, c := range s.chunks {
		b := (*buf)[:c.Size]
		if _, err := f.ReadAt(b, int64(c.Start)); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy read at %d: %w", c.Start, err)
		}
		if _, err := f.WriteAt(b, int64(offset)); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy write at %d: %w", offset, err)
		}
		offset += c.Size
	}
	return s.size(), 0, nil
}

// cloneBlocks clones the whole blocks of the move and copies the unaligned
// head and tail. The blocks are cloned first, so nothing is written if that
// fails. Plain reads and writes copy the rest, the source and destination
// don't overlap and one file handle serves as both.
func (s *inPlaceCopy) cloneBlocks(f *os.File) (copied uint64, cloned uint64, err error) {
	src, size, bs := s.srcOffset(), s.size(), s.blocksize
	lo := (src/bs + 1) * bs      // block boundary after src, as in cloneOrCopyRange
	hi := (src + size) / bs * bs // last block boundary
	if err := cloneRange(f, f, lo, hi-lo, s.dstOffset+(lo-src)); err != nil {
		return 0, 0, err
	}
	buf := getChunkBuf(bs)
	defer chunkBufPool.Put(buf)
	for _, r := range [][2]uint64{{src, lo}, {hi, src + size}} {
		b := (*buf)[:r[1]-r[0]]
		if _, err := f.ReadAt(b, int64(r[0])); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy read at %d: %w", r[0], err)
		}
		if _, err := f.WriteAt(b, int64(s.dstOffset+(r[0]-src))); err != nil {
			return 0, 0, fmt.Errorf("inPlaceCopy write at %d: %w", s.dstOffset+(r[0]-src), err)
		}
		copied += uint64(len(b))
	}
	return copied, hi - lo, nil
}

// access reads the content from before assembly, possibly into a buffer.
func (s *inPlaceCopy) access() targetAccess {
	src, size := s.srcOffset(), s.size()
	return targetAccess{
		writes:   byteRange{s.dstOffset, s.dstOffset + size},
		reads:    byteRange{src, src + size},
		readsOld: true,
		buffer:   s.buffer,
	}
}

func (s *inPlaceCopy) recordStats(stats *ExtractStats, numChunks int) {
	if s.buffer != nil && s.buffer.dropped() {
		s.fromStore().recordStats(stats, numChunks)
		return
	}
	stats.addChunksInPlace(uint64(numChunks))
}

// fromStore returns a step writing the chunk from the store, used when the
// source wasn't held in memory. Only moves of a single chunk are buffered, so
// there's just the one chunk to write.
func (s *inPlaceCopy) fromStore() *copyFromStore {
	return &copyFromStore{
		store: s.store,
		chunk: IndexChunk{ID: s.chunks[0].ID, Start: s.dstOffset, Size: s.size()},
	}
}

func (s *inPlaceCopy) String() string {
	var from string
	if s.buffer != nil {
		from = " from buffer"
	}
	src, size := s.srcOffset(), s.size()
	return fmt.Sprintf("InPlace: Copy [%d:%d] to [%d:%d]%s",
		src, src+size, s.dstOffset, s.dstOffset+size, from)
}

func (s *inPlaceCopy) Seed() Seed   { return s.seed }
func (s *inPlaceCopy) File() string { return s.file }

// Validate confirms that the source of the move holds the chunks the in-place
// seed index claims are there.
func (s *inPlaceCopy) Validate(file *os.File) error {
	if !chunksInPlace(file, s.chunks) {
		return fmt.Errorf("in-place seed index for %s doesn't match its data", s.file)
	}
	return nil
}
