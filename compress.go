package desync

import (
	"bytes"
	"io"

	"github.com/datadog/zstd"
)

// DecompressInto takes a raw (compressed) chunk as returned by the store and
// unpacks it into a provided writer. Returns the number of bytes written.
func DecompressInto(w io.Writer, b []byte) (int64, error) {
	r := zstd.NewReader(bytes.NewReader(b))
	defer r.Close()
	return io.Copy(w, r)
}
