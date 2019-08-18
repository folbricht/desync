package desync

import (
	"errors"
)

// Chunk holds chunk data compressed, uncompressed, or both. If a chunk is created
// from compressed data, such as read from a compressed chunk store, and later the
// application requires the uncompressed data, it'll be decompressed on demand and
// also stored in Chunk. The same happens when the Chunk is made from uncompressed
// bytes and then to be stored in a compressed form.
type Chunk struct {
	compressed, uncompressed []byte
	id                       ChunkID
	idCalculated             bool
}

// NewChunkFromUncompressed creates a new chunk from uncompressed data.
func NewChunkFromUncompressed(b []byte) *Chunk {
	return &Chunk{uncompressed: b}
}

// NewChunkWithID creates a new chunk from either compressed or uncompressed data
// (or both if available). It also expects an ID and validates that it matches
// the uncompressed data unless skipVerify is true. If called with just compressed
// data, it'll decompress it for the ID validation.
func NewChunkWithID(id ChunkID, uncompressed, compressed []byte, skipVerify bool) (*Chunk, error) {
	c := &Chunk{id: id, uncompressed: uncompressed, compressed: compressed}
	if skipVerify {
		c.idCalculated = true // Pretend this was calculated. No need to re-calc later
		return c, nil
	}
	sum := c.ID()
	if sum != id {
		return nil, ChunkInvalid{ID: id, Sum: sum}
	}
	return c, nil
}

// Compressed returns the chunk data in compressed form. If the chunk was created
// with uncompressed data only, it'll be compressed, stored and returned. The
// caller must not modify the data in the returned slice.
func (c *Chunk) Compressed() ([]byte, error) {
	if len(c.compressed) > 0 {
		return c.compressed, nil
	}
	if len(c.uncompressed) > 0 {
		var err error
		c.compressed, err = Compress(c.uncompressed)
		return c.compressed, err
	}
	return nil, errors.New("no data in chunk")
}

// Uncompressed returns the chunk data in uncompressed form. If the chunk was created
// with compressed data only, it'll be decompressed, stored and returned. The
// caller must not modify the data in the returned slice.
func (c *Chunk) Uncompressed() ([]byte, error) {
	if len(c.uncompressed) > 0 {
		return c.uncompressed, nil
	}
	if len(c.compressed) > 0 {
		var err error
		c.uncompressed, err = Decompress(nil, c.compressed)
		return c.uncompressed, err
	}
	return nil, errors.New("no data in chunk")
}

// ID returns the checksum/ID of the uncompressed chunk data. The ID is stored
// after the first call and doesn't need to be re-calculated. Note that calculating
// the ID may mean decompressing the data first.
func (c *Chunk) ID() ChunkID {

	if c.idCalculated {
		return c.id
	}
	b, err := c.Uncompressed()
	if err != nil {
		return ChunkID{}
	}
	c.id = Digest.Sum(b)
	c.idCalculated = true
	return c.id
}
