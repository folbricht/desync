package desync

import (
	"context"
	"fmt"
	"io"
)

// Store is a generic interface implemented by read-only stores, like SSH or
// HTTP remote stores currently.
type Store interface {
	GetChunk(id ChunkID) ([]byte, error)
	HasChunk(id ChunkID) bool
	io.Closer
	fmt.Stringer
}

// WriteStore is implemented by stores supporting both read and write operations
// such as a local store or an S3 store.
type WriteStore interface {
	Store
	StoreChunk(id ChunkID, b []byte) error
}

// PruneStore is a store that supports pruning of chunks
type PruneStore interface {
	WriteStore
	Prune(ctx context.Context, ids map[ChunkID]struct{}) error
}

type IndexStore interface {
	GetIndexReader(name string) (io.ReadCloser, error)
	GetIndex(name string) (Index, error)
	io.Closer
	fmt.Stringer
}

type IndexWriteStore interface {
	IndexStore
	StoreIndex(name string, idx Index) error
}
