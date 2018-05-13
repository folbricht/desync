package desync

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type HTTPHandler struct {
	s        Store
	writable bool
}

func NewHTTPHandler(s Store, writable bool) http.Handler {
	return HTTPHandler{s, writable}
}

func (h HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sid := strings.TrimSuffix(filepath.Base(r.URL.Path), ".cacnk")

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

	switch r.Method {
	case "GET":
		h.get(id, w)
	case "HEAD":
		h.head(id, w)
	case "PUT":
		h.put(id, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("only GET is supported"))
	}
}

func (h HTTPHandler) get(id ChunkID, w http.ResponseWriter) {
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

func (h HTTPHandler) head(id ChunkID, w http.ResponseWriter) {
	if h.s.HasChunk(id) {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (h HTTPHandler) put(id ChunkID, w http.ResponseWriter, r *http.Request) {
	// Make sure writing was enabled for this server
	if !h.writable {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "writing to upstream chunk store '%s' is not enabled\n", h.s)
		return
	}
	// The upstream store needs to support writing as well
	s, ok := h.s.(WriteStore)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "upstream chunk store '%s' does not support writing\n", h.s)
		return
	}
	// Read the chunk into memory
	b := new(bytes.Buffer)
	if _, err := io.Copy(b, r.Body); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		return
	}
	// Store it upstream
	if err := s.StoreChunk(id, b.Bytes()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
