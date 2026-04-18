package desync

import (
	"fmt"
	"net/http"
	"os"

	"github.com/pkg/errors"
)

// HTTPHandlerBase is the base object for a HTTP chunk or index store.
type HTTPHandlerBase struct {
	handlerType   string
	writable      bool
	authorization string
}

func (h HTTPHandlerBase) get(id string, b []byte, err error, w http.ResponseWriter) {
	switch err.(type) {
	case nil:
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	case ChunkMissing, NoSuchObject:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "%s %s not found", h.handlerType, id)
	default:
		fmt.Fprintf(os.Stderr, "failed to retrieve %s %s: %s\n", h.handlerType, id, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h HTTPHandlerBase) validateWritable(storeName string, w http.ResponseWriter, r *http.Request) error {
	// Make sure writing was enabled for this server
	if !h.writable {
		msg := fmt.Sprintf("writing to upstream %s store is not enabled", h.handlerType)
		http.Error(w, msg, http.StatusBadRequest)
		return errors.Errorf("writing to upstream %s store '%s' is not enabled", h.handlerType, storeName)
	}
	return nil
}
