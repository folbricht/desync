package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type chunkServerOptions struct {
	cmdStoreOptions
	stores          []string
	cache           string
	listenAddresses []string
	cert, key       string
	writable        bool
	skipVerifyWrite bool
	uncompressed    bool
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
	flags.StringVar(&opt.cert, "cert", "", "cert file in PEM format, requires --key")
	flags.StringVar(&opt.key, "key", "", "key file in PEM format, requires --cert")
	flags.BoolVarP(&opt.writable, "writeable", "w", false, "support writing")
	flags.BoolVar(&opt.skipVerify, "skip-verify-read", true, "don't verify chunk data read from upstream stores (faster)")
	flags.BoolVar(&opt.skipVerifyWrite, "skip-verify-write", true, "don't verify chunk data written to this server (faster)")
	flags.BoolVarP(&opt.uncompressed, "uncompressed", "u", false, "serve uncompressed chunks")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runChunkServer(ctx context.Context, opt chunkServerOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if (opt.key == "") != (opt.cert == "") {
		return errors.New("--key and --cert options need to be provided together")
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
	} else {
		s, err = MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	}
	if err != nil {
		return err
	}
	defer s.Close()

	http.Handle("/", desync.NewHTTPHandler(s, opt.writable, opt.skipVerifyWrite, opt.uncompressed))

	// Start the server
	return serve(ctx, opt.key, opt.cert, addresses...)
}
