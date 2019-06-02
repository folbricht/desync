package desync

import (
	"context"
	"fmt"
	"io"
	"time"
)

// Store is a generic interface implemented by read-only stores, like SSH or
// HTTP remote stores currently.
type Store interface {
	GetChunk(id ChunkID) (*Chunk, error)
	HasChunk(id ChunkID) (bool, error)
	io.Closer
	fmt.Stringer
}

// WriteStore is implemented by stores supporting both read and write operations
// such as a local store or an S3 store.
type WriteStore interface {
	Store
	StoreChunk(c *Chunk) error
}

// PruneStore is a store that supports pruning of chunks
type PruneStore interface {
	WriteStore
	Prune(ctx context.Context, ids map[ChunkID]struct{}) error
}

// IndexStore is implemented by stores that hold indexes.
type IndexStore interface {
	GetIndexReader(name string) (io.ReadCloser, error)
	GetIndex(name string) (Index, error)
	io.Closer
	fmt.Stringer
}

// IndexWriteStore is used by stores that support reading and writing of indexes.
type IndexWriteStore interface {
	IndexStore
	StoreIndex(name string, idx Index) error
}

// StoreOptions provide additional common settings used in chunk stores, such as compression
// error retry or timeouts. Not all options available are applicable to all types of stores.
type StoreOptions struct {
	// Concurrency used in the store. Depending on store type, it's used for
	// the number of goroutines, processes, or connection pool size.
	N int `json:"n,omitempty"`

	// Cert file name for HTTP SSL connections that require mutual SSL.
	ClientCert string `json:"client-cert,omitempty"`
	// Key file name for HTTP SSL connections that require mutual SSL.
	ClientKey string `json:"client-key,omitempty"`

	// CA certificates to trust in TLS connections. If not set, the systems CA store is used.
	CACert string `json:"ca-cert,omitempty"`

	// Trust any certificate presented by the remote chunk store.
	TrustInsecure bool `json:"trust-insecure,omitempty"`

	// Authorization header value for HTTP stores
	HTTPAuth string `json:"http-auth,omitempty"`

	// Timeout for waiting for objects to be retrieved. Infinite if negative. Default: 1 minute
	Timeout time.Duration `json:"timeout,omitempty"`

	// Number of times object retrieval should be attempted on error. Useful when dealing
	// with unreliable connections. Default: 0
	ErrorRetry int `json:"error-retry,omitempty"`

	// If SkipVerify is true, this store will not verfiy the data it reads and serves up. This is
	// helpful when a store is merely a proxy and the data will pass through additional stores
	// before being used. Verifying the checksum of a chunk requires it be uncompressed, so if
	// a compressed chunkstore is being proxied, all chunks would have to be decompressed first.
	// This setting avoids the extra overhead. While this could be used in other cases, it's not
	// recommended as a damaged chunk might be processed further leading to unpredictable results.
	SkipVerify bool `json:"skip-verify,omitempty"`

	// Store and read chunks uncompressed, without chunk file extension
	Uncompressed bool `json:"uncompressed"`
}
