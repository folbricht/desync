package desync

import (
	"fmt"
	"io"
	"sort"
)

// TODO: Implement WriterTo interface
//       default io.Copy implementation operates in 32k chunks; copying up to a full chunk at a time will improve perf

// IndexPos represents a position inside an index file, to permit a seeking reader
type IndexPos struct {
	Store  Store
	Index  Index
	Length int64 // total length of file
	pos    int64 // Location within offset stream; must be 0 <= Pos <= Index.

	curChunkID     ChunkID // hash of current chunk
	curChunk       []byte  // decompressed version of current chunk
	curChunkIdx    int     // identity of current chunk
	curChunkOffset int64   // offset within current chunk
	nullChunk      *NullChunk
}

func NewIndexReadSeeker(i Index, s Store) *IndexPos {
	return &IndexPos{
		Store:      s,
		Index:      i,
		Length:     i.Length(),
		curChunkID: i.Chunks[0].ID,
		nullChunk:  NewNullChunk(i.Index.ChunkSizeMax),
	}
}

/* findOffset - Actually update our IndexPos for a new Index
 *
 * - Seek forward within existing chunk if appropriate
 * - Bisect the Chunks array to find the correct chunk
 * - Decompress if id does not match curChunk
 * - Update chunkIdx and chunkOffset
 */
func (ip *IndexPos) findOffset(newPos int64) (int64, error) {
	var newChunkIdx int
	var newChunkOffset int64
	var delta int64
	var err error = nil

	// Degenerate case: Seeking to existing position
	delta = newPos - ip.pos
	if delta == 0 {
		return ip.pos, nil
	}

	// Degenerate case: Seeking within current chunk
	if (delta+ip.curChunkOffset) >= 0 &&
		(delta+ip.curChunkOffset) < int64(ip.Index.Chunks[ip.curChunkIdx].Size) {
		ip.pos += delta
		ip.curChunkOffset += delta
		return ip.pos, nil
	}

	// General case: Bisect
	chunks := ip.Index.Chunks
	newChunkIdx = sort.Search(len(chunks), func(i int) bool { return newPos < int64(chunks[i].Start+chunks[i].Size) })
	if newChunkIdx >= len(chunks) { // function was not true for any chunk -- meaning we're running off the end
		newChunkIdx = len(chunks) - 1
	}
	newChunk := ip.Index.Chunks[newChunkIdx]
	newChunkOffset = newPos - int64(newChunk.Start)

	if newPos < int64(newChunk.Start) {
		return ip.pos, fmt.Errorf("seek found chunk beginning at position %v, desired position is %v", newChunk.Start, newPos)
	}
	if newPos > int64(newChunk.Start+newChunk.Size) {
		return ip.pos, fmt.Errorf("seek found chunk ending at position %v, desired position is %v", newChunk.Start+newChunk.Size, newPos)
	}

	// Only invalidate cache if new chunk is different from old one (avoid re-decompressing all-0 regions)
	if newChunk.ID != ip.curChunkID {
		ip.curChunk = nil // next read attempt will call loadChunk()
	}
	// BELOW HERE, WE HAVE UPDATED THE DATA AND MUST NOT ERROR
	ip.curChunkIdx = newChunkIdx
	ip.curChunkID = newChunk.ID
	ip.curChunkOffset = newChunkOffset
	ip.pos = newPos
	return newPos, err
}

func (ip *IndexPos) loadChunk() (err error) {
	// See if we can simply read a blank slice from memory if the null chunk
	// is being loaded
	if ip.curChunkID == ip.nullChunk.ID {
		ip.curChunk = ip.nullChunk.Data
		return
	}

	var compressedChunk []byte
	var decompressedChunk []byte

	compressedChunk, err = ip.Store.GetChunk(ip.curChunkID)
	if err != nil {
		return err
	}

	decompressedChunk, err = Decompress(nil, compressedChunk)
	if err != nil {
		return err
	}

	ip.curChunk = decompressedChunk
	return nil
}

func (ip *IndexPos) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	var err error
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = ip.pos + offset
	case io.SeekEnd:
		newPos = ip.Length + offset
	default:
		return ip.pos, fmt.Errorf("invalid whence")
	}
	if newPos < 0 {
		return ip.pos, fmt.Errorf("unable to seek before start of file")
	}
	newOffset, err := ip.findOffset(newPos)
	if err == nil && newPos > ip.Length {
		err = io.EOF
	}
	return newOffset, err
}

func (ip *IndexPos) Read(p []byte) (n int, err error) {
	var totalCopiedBytes int
	remainingBytes := p[:]
	if ip.pos == ip.Length { // if initially called when already at the end, return EOF
		return 0, io.EOF
	}
	for len(remainingBytes) > 0 {
		if len(ip.curChunk) == 0 {
			err = ip.loadChunk()
			if err != nil {
				break
			}
		}
		chunkRemainingBytes := ip.curChunk[ip.curChunkOffset:len(ip.curChunk)]
		if len(chunkRemainingBytes) == 0 && ip.curChunkIdx == (len(ip.Index.Chunks)-1) {
			break // if running into the end after successful read, return a short read
		}
		copiedBytes := copy(remainingBytes, chunkRemainingBytes)
		remainingBytes = remainingBytes[copiedBytes:]
		totalCopiedBytes += copiedBytes
		_, err = ip.Seek(int64(copiedBytes), io.SeekCurrent)
		if err != nil {
			break
		}
	}
	return totalCopiedBytes, err
}
