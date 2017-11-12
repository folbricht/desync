package desync

import "fmt"

type Store interface {
	GetChunk(id ChunkID) ([]byte, error)
	fmt.Stringer
}
