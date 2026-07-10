package desync

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const DefaultErrorRetry = 3
const DefaultErrorRetryBaseInterval = 500 * time.Millisecond

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

// PruneStore is a store that supports read, write and pruning of chunks
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

	// Cookie header value for HTTP stores
	HTTPCookie string `json:"http-cookie,omitempty"`

	// Timeout for waiting for objects to be retrieved. Infinite if negative. Default: 1 minute
	Timeout time.Duration `json:"timeout,omitempty"`

	// Number of times object retrieval should be attempted on error. Useful when dealing
	// with unreliable connections.
	ErrorRetry int `json:"error-retry,omitempty"`

	// Number of nanoseconds to wait before first retry attempt.
	// Retry attempt number N for the same request will wait N times this interval.
	ErrorRetryBaseInterval time.Duration `json:"error-retry-base-interval,omitempty"`

	// If SkipVerify is true, this store will not verify the data it reads and serves up. This is
	// helpful when a store is merely a proxy and the data will pass through additional stores
	// before being used. Verifying the checksum of a chunk requires it be uncompressed, so if
	// a compressed chunkstore is being proxied, all chunks would have to be decompressed first.
	// This setting avoids the extra overhead. While this could be used in other cases, it's not
	// recommended as a damaged chunk might be processed further leading to unpredictable results.
	SkipVerify bool `json:"skip-verify,omitempty"`

	// Store and read chunks uncompressed, without chunk file extension
	Uncompressed bool `json:"uncompressed"`

	// Encrypt chunks in storage. The key is expected to be a hex-encoded 256-bit
	// key, for example generated with `openssl rand -hex 32`. Supported algorithms
	// are xchacha20-poly1305 (default) and aes-256-gcm.
	Encryption          bool   `json:"encryption,omitempty"`
	EncryptionAlgorithm string `json:"encryption-algorithm,omitempty"`
	EncryptionKey       string `json:"encryption-key,omitempty"`
}

// NewStoreOptionsWithDefaults creates a new StoreOptions struct with the default values set
func NewStoreOptionsWithDefaults() (o StoreOptions) {
	o.ErrorRetry = DefaultErrorRetry
	o.ErrorRetryBaseInterval = DefaultErrorRetryBaseInterval
	return o
}

func (o *StoreOptions) UnmarshalJSON(data []byte) error {
	// Set all the default values before loading the JSON store options
	o.ErrorRetry = DefaultErrorRetry
	o.ErrorRetryBaseInterval = DefaultErrorRetryBaseInterval
	type Alias StoreOptions
	return json.Unmarshal(data, (*Alias)(o))
}

// StorageConverters returns data converters that convert between plain and
// storage-format. Each layer represents a modification such as compression
// or encryption and is applied in order depending on the direction of data.
// If data is written to storage, the layer's toStorage method is called in
// the order they are returned. If data is read, the fromStorage method is
// called in reverse order.
func (o StoreOptions) StorageConverters() (Converters, error) {
	var c Converters
	if !o.Uncompressed {
		c = append(c, Compressor{})
	}
	if !o.Encryption && o.EncryptionConfigured() {
		// Refuse configs that set a key or algorithm without turning encryption
		// on. Silently writing plaintext chunks is the one failure mode this
		// feature must not have.
		return nil, errors.New("encryption-key or encryption-algorithm configured without setting encryption to true")
	}
	if o.Encryption {
		if o.EncryptionKey == "" {
			return nil, errors.New("no encryption key configured")
		}
		key, err := hex.DecodeString(o.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("invalid encryption key, expected hex-encoded 256-bit key: %w", err)
		}
		newAEAD := NewXChaCha20Poly1305
		switch o.EncryptionAlgorithm {
		case "", "xchacha20-poly1305":
		case "aes-256-gcm":
			newAEAD = NewAES256GCM
		default:
			return nil, fmt.Errorf("unsupported encryption algorithm %q", o.EncryptionAlgorithm)
		}
		enc, err := newAEAD(key)
		if err != nil {
			return nil, err
		}
		c = append(c, enc)
	}
	return c, nil
}

// EncryptionConfigured returns true if any of the encryption options is set,
// regardless of whether the combination is valid.
func (o StoreOptions) EncryptionConfigured() bool {
	return o.Encryption || o.EncryptionKey != "" || o.EncryptionAlgorithm != ""
}

// ValidateIndexOptions returns an error if the options contain settings that
// don't apply to index stores. Encryption only covers chunks, indexes are
// always stored in plain form. Index stores reject encryption options rather
// than silently ignoring them, which could be mistaken for indexes being
// stored encrypted.
func (o StoreOptions) ValidateIndexOptions() error {
	if o.EncryptionConfigured() {
		return errors.New("encryption is not supported by index stores")
	}
	return nil
}
