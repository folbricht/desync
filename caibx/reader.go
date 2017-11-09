package caibx

import (
	"encoding/binary"
	"io"

	casync "github.com/folbricht/go-casync"
)

type reader struct {
	io.Reader
}

func (r reader) ReadUint64() (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// Return the hex representation of the next 32 bytes as string
func (r reader) ReadID() (casync.ChunkID, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(r, b); err != nil {
		return casync.ChunkID{}, err
	}
	return casync.ChunkIDFromSlice(b)
}
