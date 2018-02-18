package desync

import "fmt"

// Store is a generic interface implemented by read-only stores, like SSH or
// HTTP remote stores currently.
type Store interface {
	GetChunk(id ChunkID) ([]byte, error)
	Close() error
	fmt.Stringer
}

// WriteStore is implemented by stores supporting both read and write operations
// such as a local store or an S3 store.
type WriteStore interface {
	Store
	HasChunk(id ChunkID) bool
	StoreChunk(id ChunkID, b []byte) error
}
