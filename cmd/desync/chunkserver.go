package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type chunkServerOptions struct {
	cmdStoreOptions
	cmdServerOptions
	stores          []string
	cache           string
	listenAddresses []string
	writable        bool
	skipVerifyWrite bool
	uncompressed    bool
	logFile         string
}

func newChunkServerCommand(ctx context.Context) *cobra.Command {
	var opt chunkServerOptions

	cmd := &cobra.Command{
		Use:   "chunk-server",
		Short: "Server for chunks over HTTP(S)",
		Long: `Starts an HTTP chunk server that can be used as remote store. It supports
reading from multiple local or remote stores as well as a local cache. If
--cert and --key are provided, the server will serve over HTTPS. The -w option
enables writing to this store, but this is only allowed when just one upstream
chunk store is provided. The option --skip-verify-write disables validation of
chunks written to this server which bypasses checksum validation as well as
the necessary decompression step to calculate it to improve performance. If -u
is used, only uncompressed chunks are being served (and accepted). If the
upstream store serves compressed chunks, everything will have to be decompressed 
server-side so it's better to also read from uncompressed upstream stores.

While --concurrency does not limit the number of clients that can be served
concurrently, it does influence connection pools to remote upstream stores and
needs to be chosen carefully if the server is under high load.
`,
		Example: `  desync chunk-server -s sftp://192.168.1.1/store -c /path/to/cache -l :8080`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChunkServer(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "upstream source store(s)")
	flags.StringVarP(&opt.cache, "cache", "c", "", "store to be used as cache")
	flags.StringSliceVarP(&opt.listenAddresses, "listen", "l", []string{":http"}, "listen address")
	flags.BoolVarP(&opt.writable, "writeable", "w", false, "support writing")
	flags.BoolVar(&opt.skipVerify, "skip-verify-read", true, "don't verify chunk data read from upstream stores (faster)")
	flags.BoolVar(&opt.skipVerifyWrite, "skip-verify-write", true, "don't verify chunk data written to this server (faster)")
	flags.BoolVarP(&opt.uncompressed, "uncompressed", "u", false, "serve uncompressed chunks")
	flags.StringVar(&opt.logFile, "log", "", "request log file or - for STDOUT")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	addServerOptions(&opt.cmdServerOptions, flags)
	return cmd
}

func runChunkServer(ctx context.Context, opt chunkServerOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if err := opt.cmdServerOptions.validate(); err != nil {
		return err
	}
	if opt.auth == "" {
		opt.auth = os.Getenv("DESYNC_HTTP_AUTH")
	}

	addresses := opt.listenAddresses
	if len(addresses) == 0 {
		addresses = []string{":http"}
	}

	// Checkout the store
	if len(opt.stores) == 0 {
		return errors.New("no store provided")
	}

	// When supporting writing, only one upstream store is possible
	if opt.writable && (len(opt.stores) > 1 || opt.cache != "") {
		return errors.New("Only one upstream store supported for writing")
	}

	var (
		s   desync.Store
		err error
	)
	if opt.writable {
		s, err = WritableStore(opt.stores[0], opt.cmdStoreOptions)
		if err != nil {
			return err
		}
	} else {
		s, err = MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
		if err != nil {
			return err
		}
		// We want to take the edge of a large number of requests coming in for the same chunk. No need
		// to hit the (potentially slow) upstream stores for duplicated requests.
		s = desync.NewDedupQueue(s)
	}
	defer s.Close()

	handler := desync.NewHTTPHandler(s, opt.writable, opt.skipVerifyWrite, opt.uncompressed, opt.auth)

	// Wrap the handler in a logger if requested
	switch opt.logFile {
	case "": // No logging of requests
	case "-":
		handler = withLog(handler, log.New(stderr, "", log.LstdFlags))
	default:
		l, err := os.OpenFile(opt.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer l.Close()
		handler = withLog(handler, log.New(l, "", log.LstdFlags))
	}

	http.Handle("/", handler)

	// Start the server
	return serve(ctx, opt.cmdServerOptions, addresses...)
}

// Wrapper for http.HandlerFunc to add logging for requests (and response codes)
func withLog(h http.Handler, log *log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lrw := &loggingResponseWriter{ResponseWriter: w}
		h.ServeHTTP(lrw, r)
		log.Printf("Client: %s, Request: %s %s, Response: %d", r.RemoteAddr, r.Method, r.RequestURI, lrw.statusCode)
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}
