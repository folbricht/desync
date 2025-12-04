package desync

import (
	"errors"
)

// Chunk holds chunk data plain, storage format, or both. If a chunk is created
// from storage data, such as read from a compressed chunk store, and later the
// application requires the plain data, it'll be converted on demand by applying
// the given storage converters in reverse order. The converters can only be used
// to read the plain data, not to convert back to storage format.
type Chunk struct {
	data         []byte     // Plain data if available
	storage      []byte     // Storage format (compressed, encrypted, etc)
	converters   Converters // Modifiers to convert from storage format to plain
	id           ChunkID
	idCalculated bool
}

// NewChunk creates a new chunk from plain data. The data is trusted and the ID is
// calculated on demand.
func NewChunk(b []byte) *Chunk {
	return &Chunk{data: b}
}

// NewChunkWithID creates a new chunk from either compressed or uncompressed data
// (or both if available). It also expects an ID and validates that it matches
// the uncompressed data unless skipVerify is true. If called with just compressed
// data, it'll decompress it for the ID validation.
func NewChunkWithID(id ChunkID, b []byte, skipVerify bool) (*Chunk, error) {
	c := &Chunk{id: id, data: b}
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

// NewChunkFromStorage builds a new chunk from data that is not in plain format.
// It uses raw storage format from it source and the modifiers are used to convert
// into plain data as needed.
func NewChunkFromStorage(id ChunkID, b []byte, modifiers Converters, skipVerify bool) (*Chunk, error) {
	c := &Chunk{id: id, storage: b, converters: modifiers}
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

// Data returns the chunk data in uncompressed form. If the chunk was created
// with compressed data only, it'll be decompressed, stored and returned. The
// caller must not modify the data in the returned slice.
func (c *Chunk) Data() ([]byte, error) {
	if len(c.data) > 0 {
		return c.data, nil
	}
	if len(c.storage) > 0 {
		var err error
		c.data, err = c.converters.fromStorage(c.storage)
		return c.data, err
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
	b, err := c.Data()
	if err != nil {
		return ChunkID{}
	}
	c.id = Digest.Sum(b)
	c.idCalculated = true
	return c.id
}

// Storage returns the chunk data in compressed form. If the chunk was created
// with compressed data and same modifiers, this data will be returned as is. The
// caller must not modify the data in the returned slice.
func (c *Chunk) Storage(modifiers Converters) ([]byte, error) {
	if len(c.storage) > 0 && modifiers.equal(c.converters) {
		return c.storage, nil
	}
	b, err := c.Data()
	if err != nil {
		return nil, err
	}
	return modifiers.toStorage(b)
}
