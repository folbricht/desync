package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
	"github.com/valyala/fasthttp"
)

const serverUsage = `desync chunk-server [options]

Starts an HTTP chunk server that can be used as remote store. It supports
reading from multiple local or remote stores as well as a local cache. If
-cert and -key are provided, the server will serve over HTTPS. The -w option
enables writing to this store, but this is only allowed when just one upstream
chunk store is provided.`

func server(ctx context.Context, args []string) error {
	var (
		cacheLocation   string
		n               int
		storeLocations  = new(multiArg)
		listenAddresses = new(multiArg)
		cert, key       string
		clientCert      string
		clientKey       string
		writable        bool
	)
	flags := flag.NewFlagSet("server", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, serverUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines, only used for remote SSH stores")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.Var(listenAddresses, "l", "listen address, can be multiples (default :http)")
	flags.StringVar(&cert, "cert", "", "cert file in PEM format, requires -key")
	flags.StringVar(&key, "key", "", "key file in PEM format, requires -cert")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.BoolVar(&writable, "w", false, "support writing")
	flags.Parse(args)

	if flags.NArg() > 0 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if key != "" && cert == "" || cert != "" && key == "" {
		return errors.New("-key and -cert options need to be provided together.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	if len(listenAddresses.list) == 0 {
		listenAddresses.Set(":http")
	}

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	// When supporting writing, only one upstream store is possible
	if writable && (len(storeLocations.list) > 1 || cacheLocation != "") {
		return errors.New("Only one upstream store supported for writing")
	}

	// Parse the store locations, open the stores and add a cache is requested
	var (
		s    desync.Store
		err  error
		opts = storeOptions{
			n:          n,
			clientCert: clientCert,
			clientKey:  clientKey,
		}
	)
	if writable {
		s, err = WritableStore(storeLocations.list[0], opts)
	} else {
		s, err = MultiStoreWithCache(opts, cacheLocation, storeLocations.list...)
	}
	if err != nil {
		return err
	}
	defer s.Close()

	h := desync.NewHTTPHandler(s, writable)

	// Run the server(s) in a goroutine, and use the main goroutine to wait for
	// a signal or a failing server (ctx gets cancelled in that case)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, addr := range listenAddresses.list {
		go func(a string) {
			server := &fasthttp.Server{
				Handler: h.HandleFastHTTP,
			}
			var err error
			if key == "" {
				err = server.ListenAndServe(a)
			} else {
				err = server.ListenAndServeTLS(a, cert, key)
			}
			fmt.Fprintln(os.Stderr, err)
			cancel()
		}(addr)
	}
	// wait for either INT/TERM or an issue with the server
	<-ctx.Done()
	return nil
}
