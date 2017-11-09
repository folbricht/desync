package casync

import "fmt"

// ChunkMissing is returned by a store that can't find a requested chunk
type ChunkMissing struct {
	ID ChunkID
}

func (e ChunkMissing) Error() string {
	return fmt.Sprintf("chunk %s missing from store", e.ID)
}
