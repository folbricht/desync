package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/folbricht/desync"
)

const serverUsage = `desync chunk-server [options]

Starts an HTTP chunk server that can be used as remote store. It supports
reading from multiple local or remote stores as well as a local cache.`

func server(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		storeLocations = new(multiArg)
		stores         []desync.Store
		listenInt      string
	)
	flags := flag.NewFlagSet("server", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, serverUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines, only used for remote SSH stores")
	flags.StringVar(&listenInt, "l", ":http", "listen address")
	flags.Parse(args)

	if flags.NArg() > 0 {
		return errors.New("Too many arguments. See -h for help.")
	}

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	// Go through each store passed in the command line, initialize them, and
	// build a list
	for _, location := range storeLocations.list {
		loc, err := url.Parse(location)
		if err != nil {
			return fmt.Errorf("Unable to parse store location %s : %s", location, err)
		}
		var s desync.Store
		switch loc.Scheme {
		case "ssh":
			r, err := desync.NewRemoteSSHStore(loc, n)
			if err != nil {
				return err
			}
			defer r.Close()
			s = r
		case "http", "https":
			s, err = desync.NewRemoteHTTPStore(loc)
			if err != nil {
				return err
			}
		case "":
			s, err = desync.NewLocalStore(loc.Path)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("Unsupported store access scheme %s", loc.Scheme)
		}
		stores = append(stores, s)
	}

	// Combine all stores into one router
	var s desync.Store = desync.NewStoreRouter(stores...)

	// See if we want to use a local store as cache, if so, attach a cache to
	// the router
	if cacheLocation != "" {
		cache, err := desync.NewLocalStore(cacheLocation)
		if err != nil {
			return err
		}
		cache.UpdateTimes = true
		s = desync.NewCache(s, cache)
	}

	http.Handle("/", desync.NewHTTPHandler(s))

	server := &http.Server{
		Addr: listenInt,
	}

	// Run the server in a goroutine, and use the main goroutine to wait for
	// a signal (ctx gets cancelled) and shutdown the server cleanly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		err := server.ListenAndServe()
		// Did the HTTP server stop because we called shutdown or some problem?
		if err != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, err)
			cancel()
		}
	}()

	// wait for either INT/TERM or an issue with the server
	<-ctx.Done()
	return server.Shutdown(context.Background())
}
