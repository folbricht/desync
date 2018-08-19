package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"

	"strings"

	"github.com/folbricht/desync"
)

type ServerType int

const (
	IndexServer = iota
	ChunkServer
)

const (
	chunkServerUsage = `desync chunk-server [options]

Starts an HTTP chunk server that can be used as remote store. It supports
reading from multiple local or remote stores as well as a local cache. If
-cert and -key are provided, the server will serve over HTTPS. The -w option
enables writing to this store, but this is only allowed when just one upstream
chunk store is provided.`

	indexServerUsage = `desync index-server [options]

Starts an HTTP index server that can be used as remote store. It supports
reading from a single local or remote store.
If -cert and -key are provided, the server will serve over HTTPS. The -w option
enables writing to this store.`
)

func server(ctx context.Context, serverType ServerType, args []string) error {
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
		if serverType == ChunkServer {
			fmt.Fprintln(os.Stderr, chunkServerUsage)
		}

		if serverType == IndexServer {
			fmt.Fprintln(os.Stderr, indexServerUsage)
		}
		flags.PrintDefaults()
	}

	if serverType == ChunkServer {
		flags.Var(storeLocations, "s", "casync store location, can be multiples")
		flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	}

	if serverType == IndexServer {
		flags.Var(storeLocations, "s", "index store location")
	}

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
		return errors.New("No store provided. See -h for help.")
	}

	// When supporting writing, only one upstream store is possible
	if writable && (len(storeLocations.list) > 1 || cacheLocation != "") {
		return errors.New("Only one upstream store supported for writing")
	}

	// Parse the store locations, open the stores and add a cache is requested
	var (
		opts = storeOptions{
			n:          n,
			clientCert: clientCert,
			clientKey:  clientKey,
		}
	)

	if serverType == ChunkServer {
		s, err := handleChunkStore(writable, storeLocations, opts, cacheLocation)
		if err != nil {
			return err
		}
		defer s.Close()
	}

	if serverType == IndexServer {
		is, err := handleIndexStore(writable, storeLocations, opts, cacheLocation)
		if err != nil {
			return err
		}
		defer is.Close()
	}

	// Run the server(s) in a goroutine, and use the main goroutine to wait for
	// a signal or a failing server (ctx gets cancelled in that case)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, addr := range listenAddresses.list {
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

func handleChunkStore(writable bool, storeLocations *multiArg, opts storeOptions, cacheLocation string) (desync.Store, error) {
	var (
		s   desync.Store
		err error
	)
	if writable {
		s, err = WritableStore(storeLocations.list[0], opts)
	} else {
		s, err = MultiStoreWithCache(opts, cacheLocation, storeLocations.list...)
	}
	if err != nil {
		return nil, err
	}

	http.Handle("/", desync.NewHTTPHandler(s, writable))
	return s, err
}

func handleIndexStore(writable bool, storeLocations *multiArg, opts storeOptions, cacheLocation string) (desync.IndexStore, error) {
	var (
		s   desync.IndexStore
		err error
	)

	// Making sure we have a "/" at the end
	loc := storeLocations.list[0]
	if !strings.HasSuffix(loc, "/") {
		loc = loc + "/"
	}

	if writable {
		s, _, err = writableIndexStore(loc, opts)
	} else {
		s, _, err = indexStoreFromLocation(loc, opts)
	}
	if err != nil {
		return nil, err
	}

	http.Handle("/", desync.NewHTTPIndexHandler(s, writable))
	return s, err
}
