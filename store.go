package desync

import (
	"context"
	"fmt"
)

// Store is a generic interface implemented by read-only stores, like SSH or
// HTTP remote stores currently.
type Store interface {
	GetChunk(id ChunkID) ([]byte, error)
	Close() error
	fmt.Stringer
}

// QueryStore implements functions to check if a chunk is in the store
type QueryStore interface {
	HasChunk(id ChunkID) bool
}

// WriteStore is implemented by stores supporting both read and write operations
// such as a local store or an S3 store.
type WriteStore interface {
	Store
	QueryStore
	StoreChunk(id ChunkID, b []byte) error
	Prune(ctx context.Context, ids map[ChunkID]struct{}) error
}
