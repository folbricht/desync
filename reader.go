package desync

import (
	"encoding/binary"
	"io"
)

type reader struct {
	io.Reader
}

// ReadUint64 reads the next 8 bytes from the reader and returns it as little
// endian Uint64
func (r reader) ReadUint64() (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// ReadN returns the next n bytes from the reader or an error if there are not
// enough left
func (r reader) ReadN(n uint64) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadID reads and returns a ChunkID
func (r reader) ReadID() (ChunkID, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(r, b); err != nil {
		return ChunkID{}, err
	}
	return ChunkIDFromSlice(b)
}
