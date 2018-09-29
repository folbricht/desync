package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type indexServerOptions struct {
	cmdStoreOptions
	store           string
	listenAddresses []string
	cert, key       string
	writable        bool
}

func newIndexServerCommand(ctx context.Context) *cobra.Command {
	var opt indexServerOptions

	cmd := &cobra.Command{
		Use:   "index-server",
		Short: "Server for indexes over HTTP(S)",
		Long: `Starts an HTTP index server that can be used as remote store. It supports
reading from a single local or a proxying to a remote store.
If --cert and --key are provided, the server will serve over HTTPS. The -w option
enables writing to this store.`,
		Example: `  desync index-server -s sftp://192.168.1.1/indexes -l :8080`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIndexServer(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "upstream source index store")
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	flags.StringSliceVarP(&opt.listenAddresses, "listen", "l", []string{":http"}, "listen address")
	flags.StringVar(&opt.cert, "cert", "", "cert file in PEM format, requires --key")
	flags.StringVar(&opt.key, "key", "", "key file in PEM format, requires --cert")
	flags.BoolVarP(&opt.writable, "writeable", "w", false, "support writing")
	return cmd
}

func runIndexServer(ctx context.Context, opt indexServerOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	if (opt.key == "") != (opt.cert == "") {
		return errors.New("--key and --cert options need to be provided together")
	}

	addresses := opt.listenAddresses
	if len(addresses) == 0 {
		addresses = []string{":http"}
	}

	// Checkout the store
	if opt.store == "" {
		return errors.New("no store provided")
	}

	// Making sure we have a "/" at the end
	loc := opt.store
	if !strings.HasSuffix(loc, "/") {
		loc = loc + "/"
	}

	var (
		s   desync.IndexStore
		err error
	)
	if opt.writable {
		s, _, err = writableIndexStore(loc, opt.cmdStoreOptions)
	} else {
		s, _, err = indexStoreFromLocation(loc, opt.cmdStoreOptions)
	}
	if err != nil {
		return err
	}
	defer s.Close()

	// Setup the handler for the index server
	http.Handle("/", desync.NewHTTPIndexHandler(s, opt.writable))

	// Start the server
	return serve(ctx, opt.key, opt.cert, addresses...)
}

func serve(ctx context.Context, key, cert string, addresses ...string) error {
	// Run the server(s) in a goroutine, and use the main goroutine to wait for
	// a signal or a failing server (ctx gets cancelled in that case)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, addr := range addresses {
		go func(a string) {
			server := &http.Server{Addr: a}
			var err error
			if key == "" {
				err = server.ListenAndServe()
			} else {
				err = server.ListenAndServeTLS(cert, key)
			}
			fmt.Fprintln(os.Stderr, err)
			cancel()
		}(addr)
	}
	// wait for either INT/TERM or an issue with the server
	<-ctx.Done()
	return nil
}
