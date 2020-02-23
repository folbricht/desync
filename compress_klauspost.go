// +build klauspost

package desync

import "github.com/klauspost/compress/zstd"

// Create a reader/writer that caches compressors.
var (
	encoder, _ = zstd.NewWriter(nil)
	decoder, _ = zstd.NewReader(nil)
)

// Compress a block using the only (currently) supported algorithm
func Compress(src []byte) ([]byte, error) {
	return encoder.EncodeAll(src, make([]byte, 0, len(src))), nil
}

// Decompress a block using the only supported algorithm. If you already have
// a buffer it can be passed into out and will be used. If out=nil, a buffer
// will be allocated.
func Decompress(dst, src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}
