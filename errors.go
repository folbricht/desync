package desync

import "fmt"

// ChunkMissing is returned by a store that can't find a requested chunk
type ChunkMissing struct {
	ID ChunkID
}

func (e ChunkMissing) Error() string {
	return fmt.Sprintf("chunk %s missing from store", e.ID)
}

// ChunkInvalid means the hash of the chunk content doesn't match its ID
type ChunkInvalid struct {
	ID  ChunkID
	Sum ChunkID
}

func (e ChunkInvalid) Error() string {
	return fmt.Sprintf("chunk id %s does not match its hash %s", e.ID, e.Sum)
}
