package desync

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"path"
)

// HTTPIndexHandler is the HTTP handler for index stores.
type HTTPIndexHandler struct {
	HTTPHandlerBase
	s IndexStore
}

// NewHTTPIndexHandler initializes an HTTP index store handler
func NewHTTPIndexHandler(s IndexStore, writable bool, auth string) http.Handler {
	return HTTPIndexHandler{HTTPHandlerBase{"index", writable, auth}, s}
}

func (h HTTPIndexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.authorization != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(h.authorization)) != 1 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	indexName := path.Base(r.URL.Path)

	switch r.Method {
	case "GET":
		h.get(indexName, w)
	case "HEAD":
		h.head(indexName, w)
	case "PUT":
		h.put(indexName, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("only GET, PUT and HEAD are supported"))
	}
}

func (h HTTPIndexHandler) get(indexName string, w http.ResponseWriter) {
	idx, err := h.s.GetIndex(indexName)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "index not found", http.StatusNotFound)
			return
		}
		fmt.Fprintf(os.Stderr, "failed to retrieve index %s: %s\n", indexName, err)
		http.Error(w, "failed to retrieve index", http.StatusBadRequest)
		return
	}
	b := new(bytes.Buffer)
	_, err = idx.WriteTo(b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to serialize index %s: %s\n", indexName, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	h.HTTPHandlerBase.get(indexName, b.Bytes(), err, w)
}

func (h HTTPIndexHandler) head(indexName string, w http.ResponseWriter) {
	_, err := h.s.GetIndexReader(indexName)
	if err != nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (h HTTPIndexHandler) put(indexName string, w http.ResponseWriter, r *http.Request) {
	err := h.HTTPHandlerBase.validateWritable(h.s.String(), w, r)
	if err != nil {
		return
	}

	// The upstream store needs to support writing as well
	s, ok := h.s.(IndexWriteStore)
	if !ok {
		fmt.Fprintf(os.Stderr, "upstream index store '%s' does not support writing\n", h.s)
		http.Error(w, "upstream index store does not support writing", http.StatusBadRequest)
		return
	}

	// Read the chunk into memory
	idx, err := IndexFromReader(r.Body)
	if err != nil {
		http.Error(w, "invalid index: "+err.Error(), http.StatusUnsupportedMediaType)
		return
	}

	// Store it upstream
	if err := s.StoreIndex(indexName, idx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to store index %s: %s\n", indexName, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
