package desync

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/valyala/fasthttp"
)

type HTTPHandler struct {
	s        Store
	writable bool
}

func NewHTTPHandler(s Store, writable bool) HTTPHandler {
	return HTTPHandler{s, writable}
}

func (h HTTPHandler) HandleFastHTTP(ctx *fasthttp.RequestCtx) {
	sid := strings.TrimSuffix(filepath.Base(string(ctx.RequestURI())), chunkFileExt)

	// Parse the ID and verify the format
	id, err := ChunkIDFromString(sid)
	if err != nil {
		ctx.Error("invalid chunk id", fasthttp.StatusBadRequest)
		return
	}

	// We only really need the ID, but to maintain compatibility with stores
	// that are simply shared with HTTP, we expect /prefix/chunkID. Make sure
	// the prefix does match the first characters of the ID.
	if string(ctx.Path()) != filepath.Join(string(os.PathSeparator), sid[0:4], sid+chunkFileExt) {
		ctx.Error("expected /prefix/chunkid.cacnk", fasthttp.StatusBadRequest)
		return
	}

	switch string(ctx.Method()) {
	case "GET":
		h.get(id, ctx)
	case "HEAD":
		h.head(id, ctx)
	case "PUT":
		h.put(id, ctx)
	default:
		ctx.Error("only GET, HEAD, PUT are supported", fasthttp.StatusMethodNotAllowed)
	}
}

func (h HTTPHandler) get(id ChunkID, ctx *fasthttp.RequestCtx) {
	b, err := h.s.GetChunk(id)
	switch err.(type) {
	case nil:
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetContentType("application/octet-stream")
		ctx.SetBody(b)
	case ChunkMissing:
		ctx.Error(fmt.Sprintf("chunk %s not found", id), fasthttp.StatusNotFound)
	default:
		msg := fmt.Sprintf("failed to retrieve chunk %s:%s", id, err)
		ctx.Error(msg, fasthttp.StatusInternalServerError)
		fmt.Fprintln(os.Stderr, msg)
	}
}

func (h HTTPHandler) head(id ChunkID, ctx *fasthttp.RequestCtx) {
	if h.s.HasChunk(id) {
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusNotFound)
}

func (h HTTPHandler) put(id ChunkID, ctx *fasthttp.RequestCtx) {
	// Make sure writing was enabled for this server
	if !h.writable {
		ctx.Error(fmt.Sprintf("writing to upstream chunk store '%s' is not enabled\n", h.s), fasthttp.StatusBadRequest)
		return
	}
	// The upstream store needs to support writing as well
	s, ok := h.s.(WriteStore)
	if !ok {
		ctx.Error(fmt.Sprintf("upstream chunk store '%s' does not support writing\n", h.s), fasthttp.StatusBadRequest)
		return
	}
	// Store the chunk
	if err := s.StoreChunk(id, ctx.PostBody()); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		fmt.Fprintln(os.Stderr, err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}
