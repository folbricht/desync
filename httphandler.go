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

// HTTPHandler is the server-side handler for a HTTP chunk store.
type HTTPHandler struct {
	HTTPHandlerBase
	s               Store
	SkipVerifyWrite bool
}

// NewHTTPHandler initializes and returns a new HTTP handler for a chunks erver.
func NewHTTPHandler(s Store, writable, skipVerifyWrite bool) http.Handler {
	return HTTPHandler{HTTPHandlerBase{"chunk", writable}, s, skipVerifyWrite}
}

func (h HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sid := strings.TrimSuffix(filepath.Base(r.URL.Path), ".cacnk")

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
		h.get(sid, w)
	case "HEAD":
		h.head(sid, w)
	case "PUT":
		h.put(sid, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("only GET, PUT and HEAD are supported"))
	}
}

func (h HTTPHandler) parseChunkID(sid string, w http.ResponseWriter) (ChunkID, error) {
	// Parse the ID and verify the format
	cid, err := ChunkIDFromString(sid)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid chunk id"))
		return ChunkID{}, err
	}
	return cid, err
}

func (h HTTPHandler) get(sid string, w http.ResponseWriter) {
	cid, err := h.parseChunkID(sid, w)
	if err != nil {
		return
	}
	var b []byte
	chunk, err := h.s.GetChunk(cid)
	if err == nil {
		var e error
		b, e = chunk.Compressed()
		if e != nil {
			err = e
		}
	}
	h.HTTPHandlerBase.get(sid, b, err, w)
}

func (h HTTPHandler) head(sid string, w http.ResponseWriter) {
	cid, err := h.parseChunkID(sid, w)
	if err != nil {
		return
	}
	if h.s.HasChunk(cid) {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (h HTTPHandler) put(sid string, w http.ResponseWriter, r *http.Request) {
	err := h.HTTPHandlerBase.validateWritable(h.s.String(), w, r)
	if err != nil {
		return
	}

	cid, err := h.parseChunkID(sid, w)
	if err != nil {
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

	// Turn it into a chunk, and validate the ID unless verification is disabled
	chunk, err := NewChunkWithID(cid, nil, b.Bytes(), h.SkipVerifyWrite)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Store it upstream
	if err := s.StoreChunk(chunk); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
