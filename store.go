package desync

import "fmt"

type Store interface {
	GetChunk(id ChunkID) ([]byte, error)
	Close() error
	fmt.Stringer
}
