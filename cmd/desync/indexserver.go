package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type indexServerOptions struct {
	cmdStoreOptions
	cmdServerOptions
	store           string
	listenAddresses []string
	writable        bool
	logFile         string
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
	flags.StringSliceVarP(&opt.listenAddresses, "listen", "l", []string{":http"}, "listen address")
	flags.BoolVarP(&opt.writable, "writeable", "w", false, "support writing")
	flags.StringVar(&opt.logFile, "log", "", "request log file or - for STDOUT")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	addServerOptions(&opt.cmdServerOptions, flags)
	return cmd
}

func runIndexServer(ctx context.Context, opt indexServerOptions, args []string) error {
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

	handler := desync.NewHTTPIndexHandler(s, opt.writable, opt.auth)

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

	// Start the server
	return serve(ctx, handler, opt.cmdServerOptions, addresses...)
}

func serve(ctx context.Context, handler http.Handler, opt cmdServerOptions, addresses ...string) error {
	tlsConfig := &tls.Config{}
	if opt.mutualTLS {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	if opt.clientCA != "" {
		certPool := x509.NewCertPool()
		b, err := ioutil.ReadFile(opt.clientCA)
		if err != nil {
			return err
		}
		if ok := certPool.AppendCertsFromPEM(b); !ok {
			return errors.New("no client CA certficates found in client-ca file")
		}
		tlsConfig.ClientCAs = certPool
	}

	// Wrap the handler to allow H2C and handle incoming requests for HTTP2 over plain HTTP
	// connections without TLS.
	h2 := &http2.Server{}
	handler = h2c.NewHandler(handler, h2)

	// Run the server(s) in a goroutine, and use the main goroutine to wait for
	// a signal or a failing server (ctx gets cancelled in that case)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, addr := range addresses {
		go func(a string) {
			server := &http.Server{
				Addr:      a,
				TLSConfig: tlsConfig,
				ErrorLog:  log.New(stderr, "", log.LstdFlags),
				Handler:   handler,
			}
			var err error
			if opt.key == "" {
				err = server.ListenAndServe()
			} else {
				err = server.ListenAndServeTLS(opt.cert, opt.key)
			}
			fmt.Fprintln(stderr, err)
			cancel()
		}(addr)
	}
	// wait for either INT/TERM or an issue with the server
	<-ctx.Done()
	return nil
}
