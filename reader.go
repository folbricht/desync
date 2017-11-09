package casync

import (
	"encoding/binary"
	"io"
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

func (r reader) ReadN(n uint64) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}
