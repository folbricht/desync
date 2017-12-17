package desync

import (
	"fmt"

	"github.com/pkg/errors"

	"io"
)

// Index represents the content of a caibx file
type Index struct {
	Index  FormatIndex
	Chunks []IndexChunk
}

// IndexChunk is a table entry in an index file containing the chunk ID (SHA256)
// Similar to an FormatTableItem but with Start and Size instead of just offset to
// make it easier to use throughout the application.
type IndexChunk struct {
	ID    ChunkID
	Start uint64
	Size  uint64
}

// IndexFromReader parses a caibx structure (from a reader) and returns a populated Caibx
// object
func IndexFromReader(r io.Reader) (c Index, err error) {
	d := NewFormatDecoder(r)
	var ok bool
	// Read the index
	e, err := d.Next()
	if err != nil {
		return c, errors.Wrap(err, "reading index")
	}

	c.Index, ok = e.(FormatIndex)
	if !ok {
		return c, errors.New("input is not an index file")
	}

	if c.Index.FeatureFlags&CaFormatSHA512256 == 0 {
		return c, errors.New("only SHA512/256 is supported")
	}

	// Read the table
	e, err = d.Next()
	if err != nil {
		return c, errors.Wrap(err, "reading chunk table")
	}
	table, ok := e.(FormatTable)
	if !ok {
		return c, errors.New("index table not found in input")
	}

	// Convert the chunk table into a different format for easier use
	c.Chunks = make([]IndexChunk, len(table.Items))
	var lastOffset uint64
	for i, r := range table.Items {
		c.Chunks[i].ID = r.Chunk
		c.Chunks[i].Start = lastOffset
		c.Chunks[i].Size = r.Offset - lastOffset
		lastOffset = r.Offset
		// Check the max size of the chunk only. The min apperently doesn't apply
		// to the last chunk.
		if c.Chunks[i].Size > c.Index.ChunkSizeMax {
			return c, fmt.Errorf("chunk size %d is larger than maximum %d", c.Chunks[i].Size, c.Index.ChunkSizeMax)
		}
	}
	return
}
