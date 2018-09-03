package desync

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// HTTPIndexHandler is the HTTP handler for index stores.
type HTTPIndexHandler struct {
	HTTPHandlerBase
	s IndexStore
}

// NewHTTPIndexHandler initializes an HTTP index store handler
func NewHTTPIndexHandler(s IndexStore, writable bool) http.Handler {
	return HTTPIndexHandler{HTTPHandlerBase{"index", writable}, s}
}

func (h HTTPIndexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	indexName := filepath.Base(r.URL.Path)

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
	ir, err := h.s.GetIndexReader(indexName)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		fmt.Fprintln(w, err)
		return
	}
	b := new(bytes.Buffer)
	_, err = b.ReadFrom(ir)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
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
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "upstream index store '%s' does not support writing\n", h.s)
		return
	}

	// Read the chunk into memory
	idx, err := IndexFromReader(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		return
	}

	// Store it upstream
	if err := s.StoreIndex(indexName, idx); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
