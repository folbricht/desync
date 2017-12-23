package desync

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type HTTPHandler struct {
	s Store
}

func NewHTTPHandler(s Store) http.Handler {
	return HTTPHandler{s}
}

func (h HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("only GET is supported"))
		return
	}

	sid := strings.TrimSuffix(filepath.Base(r.URL.Path), ".cacnk")

	// Verify the lenth of the ID
	if len(sid) != 64 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid chunk id"))
		return
	}

	// Parse the ID and verify the format
	id, err := ChunkIDFromString(sid)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid chunk id"))
		return
	}

	// We only really need the ID, but to maintain compatibility with stores
	// that are simply shared with HTTP, we expect /prefix/chunkID. Make sure
	// the prefix does match the first characters of the ID.
	if r.URL.Path != filepath.Join(string(os.PathSeparator), sid[0:4], sid+".cacnk") {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("expected /prefix/chunkid.cacnk"))
		return
	}

	b, err := h.s.GetChunk(id)
	switch err.(type) {
	case nil:
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	case ChunkMissing:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "chunk %s not found", id)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("failed to retrieve chunk %s:%s", id, err)
		fmt.Fprintln(w, msg)
		fmt.Fprintln(os.Stderr, msg)
	}
}
