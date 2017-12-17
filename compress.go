package desync

import (
	"github.com/datadog/zstd"
)

// Compress a block using the only (currently) supported algorithm
func Compress(b []byte) ([]byte, error) {
	return zstd.CompressLevel(nil, b, 3)
}

// Decompress a block using the only supported algorithm. If you already have
// a buffer it can be passed into out and will be used. If out=nil, a buffer
// will be allocated.
func Decompress(out, in []byte) ([]byte, error) {
	return zstd.Decompress(out, in)
}
