package desync

import (
	"crypto/sha512"
)

// NullChunk is used in places where it's common to see requests for chunks
// containing only 0-bytes. When a chunked file has large areas of 0-bytes,
// the chunking algorithm does not produce split boundaries, which results
// in many chunks of 0-bytes of size MAX (max chunk size). The NullChunk can be
// used to make requesting this kind of chunk more efficient by serving it
// from memory, rather that request it from disk or network and decompress
// it repeatedly.
type NullChunk struct {
	Data []byte
	ID   ChunkID
}

// NewNullChunk returns an initialized chunk consisting of 0-bytes of 'size'
// which must mach the max size used in the index to be effective
func NewNullChunk(size uint64) *NullChunk {
	b := make([]byte, int(size))
	return &NullChunk{
		Data: b,
		ID:   sha512.Sum512_256(b),
	}
}
