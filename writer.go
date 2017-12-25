package desync

import (
	"bytes"
	"encoding/binary"
	"io"
)

type writer struct {
	io.Writer
}

// WriteUint64 converts a number of uint64 values into bytes and writes them
// into the stream. Simplifies working with the casync format since almost
// everything is expressed as uint64.
func (w writer) WriteUint64(values ...uint64) (int64, error) {
	b := make([]byte, 8*len(values))
	for i, v := range values {
		binary.LittleEndian.PutUint64(b[i*8:i*8+8], v)
	}
	return io.Copy(w, bytes.NewReader(b))
}

// WriteID serializes a ChunkID into a stream
func (w writer) WriteID(c ChunkID) (int64, error) {
	return io.Copy(w, bytes.NewReader(c[:]))
}
