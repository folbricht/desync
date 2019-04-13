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
	Authorization string
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
		w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("failed to retrieve %s %s:%s", h.handlerType, id, err)
		fmt.Fprintln(w, msg)
		fmt.Fprintln(os.Stderr, msg)
	}
}

func (h HTTPHandlerBase) validateWritable(storeName string, w http.ResponseWriter, r *http.Request) error {
	// Make sure writing was enabled for this server
	if !h.writable {
		w.WriteHeader(http.StatusBadRequest)
		msg := fmt.Sprintf("writing to upstream %s store '%s' is not enabled", h.handlerType, storeName)
		fmt.Fprintln(w, msg)
		return errors.New(msg)
	}
	return nil
}
