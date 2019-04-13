package desync

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/pkg/errors"
)

// HTTPHandler is the server-side handler for a HTTP chunk store.
type HTTPHandler struct {
	HTTPHandlerBase
	s               Store
	SkipVerifyWrite bool
	Uncompressed    bool
}

// NewHTTPHandler initializes and returns a new HTTP handler for a chunks erver.
func NewHTTPHandler(s Store, writable, skipVerifyWrite, uncompressed bool, auth string) http.Handler {
	return HTTPHandler{HTTPHandlerBase{"chunk", writable, auth}, s, skipVerifyWrite, uncompressed}
}

func (h HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Authorization != "" && r.Header.Get("Authorization") != h.Authorization {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	id, err := h.idFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		w.Write([]byte("only GET, PUT and HEAD are supported"))
	}
}

func (h HTTPHandler) get(id ChunkID, w http.ResponseWriter) {
	var b []byte
	chunk, err := h.s.GetChunk(id)
	if err == nil {
		if h.Uncompressed {
			b, err = chunk.Uncompressed()
		} else {
			b, err = chunk.Compressed()
		}
	}
	h.HTTPHandlerBase.get(id.String(), b, err, w)
}

func (h HTTPHandler) head(id ChunkID, w http.ResponseWriter) {
	if h.s.HasChunk(id) {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (h HTTPHandler) put(id ChunkID, w http.ResponseWriter, r *http.Request) {
	err := h.HTTPHandlerBase.validateWritable(h.s.String(), w, r)
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
	var chunk *Chunk
	if h.Uncompressed {
		chunk, err = NewChunkWithID(id, b.Bytes(), nil, h.SkipVerifyWrite)
	} else {
		chunk, err = NewChunkWithID(id, nil, b.Bytes(), h.SkipVerifyWrite)
	}
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

func (h HTTPHandler) idFromPath(p string) (ChunkID, error) {
	ext := CompressedChunkExt
	if h.Uncompressed {
		if strings.HasSuffix(p, CompressedChunkExt) {
			return ChunkID{}, errors.New("compressed chunk requested from http chunk store serving uncompressed chunks")
		}
		ext = UncompressedChunkExt
	}
	sID := strings.TrimSuffix(path.Base(p), ext)
	if len(sID) < 4 {
		return ChunkID{}, fmt.Errorf("expected format '/<prefix>/<chunkid>%s", ext)
	}

	// Make sure the prefix does match the first characters of the ID.
	if p != path.Join("/", sID[0:4], sID+ext) {
		return ChunkID{}, fmt.Errorf("expected format '/<prefix>/<chunkid>%s", ext)
	}
	return ChunkIDFromString(sID)
}
