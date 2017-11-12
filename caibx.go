package desync

import (
	"fmt"

	"github.com/pkg/errors"

	"io"
	"math"
)

// Caibx represents the content of a caibx file
type Caibx struct {
	Index  Index
	Header Header
	Chunks []BlobIndexChunk
}

// Index at the start of the caibx file
type Index struct {
	Size         uint64
	Type         uint64
	Flags        uint64
	ChunkSizeMin uint64
	ChunkSizeAvg uint64
	ChunkSizeMax uint64
}

// Header follows the Index in the caibx file
type Header struct {
	Size uint64
	Type uint64
}

// BlobIndexChunk is a table entry in a caibx file containing the chunk ID (SHA256)
// as well as the offset within the blob after appending this chunk
type BlobIndexChunk struct {
	Start uint64
	Size  uint64
	ID    ChunkID
}

// CaibxFromReader parses a caibx structure (from a reader) and returns a populated Caibx
// object
func CaibxFromReader(r io.Reader) (c Caibx, err error) {
	cr := reader{r}

	// Read the index
	c.Index, err = readIndex(cr)
	if err != nil {
		return c, errors.Wrap(err, "reading index")
	}

	if c.Index.Type != CaFormatIndex {
		return c, errors.New("Only blob indexes are supported")
	}

	// Read the header
	c.Header, err = readHeader(cr)
	if err != nil {
		return c, errors.Wrap(err, "reading header")
	}
	if c.Header.Type != CaFormatHeader {
		return c, errors.New("Expected table header")
	}
	if c.Header.Size != math.MaxUint64 {
		return c, errors.New("Expected MAX_UINT64 at top of table")
	}

	// And the chunks
	c.Chunks, err = readChunks(cr, c.Index.ChunkSizeMax)
	if err != nil {
		return c, errors.Wrap(err, "reading table")
	}
	return
}

func readIndex(r reader) (i Index, err error) {
	i.Size, err = r.ReadUint64()
	if err != nil {
		return
	}
	i.Type, err = r.ReadUint64()
	if err != nil {
		return
	}
	i.Flags, err = r.ReadUint64()
	if err != nil {
		return
	}
	i.ChunkSizeMin, err = r.ReadUint64()
	if err != nil {
		return
	}
	i.ChunkSizeAvg, err = r.ReadUint64()
	if err != nil {
		return
	}
	i.ChunkSizeMax, err = r.ReadUint64()
	return
}

func readHeader(r reader) (h Header, err error) {
	h.Size, err = r.ReadUint64()
	if err != nil {
		return
	}
	h.Type, err = r.ReadUint64()
	return
}

func readChunks(r reader, max uint64) (chunks []BlobIndexChunk, err error) {
	var lastOffset uint64
	for {
		var (
			c      BlobIndexChunk
			offset uint64
		)
		offset, err = r.ReadUint64()
		if err != nil {
			return
		}
		if offset == 0 { // Last chunk?
			break
		}
		c.Start = lastOffset
		c.Size = offset - lastOffset
		// Check the max size of the chunk only. The min apperently doesn't apply
		// to the last chunk.
		if c.Size > max {
			return chunks, fmt.Errorf("chunk size %d is larger than maximum %d", c.Size, max)
		}
		c.ID, err = r.ReadID()
		if err != nil {
			return
		}
		lastOffset = offset
		chunks = append(chunks, c)
	}
	return
}
