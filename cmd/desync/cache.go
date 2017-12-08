package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sync"

	"github.com/folbricht/desync"
)

const cacheUsage = `desync cache [options] <caibx> [<caibx>..]

Read chunk IDs in caibx files from one or more stores without creating a blob.
Can be used to pre-populate a local cache.`

func cache(args []string) {
	var (
		cacheLocation  string
		n              int
		storeLocations = new(multiArg)
		stores         []desync.Store
	)
	flags := flag.NewFlagSet("cache", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, cacheUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.Parse(args)

	if flags.NArg() < 1 {
		die(errors.New("Not enough arguments. See -h for help."))
	}

	// Checkout the store
	if len(storeLocations.list) == 0 {
		die(errors.New("No casync store provided. See -h for help."))
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	ids := make(map[desync.ChunkID]struct{})
	for _, name := range flags.Args() {
		c, err := readCaibxFile(name)
		if err != nil {
			die(err)
		}
		for _, c := range c.Chunks {
			ids[c.ID] = struct{}{}
		}
	}

	// Go through each stored passed in the command line, initialize them, and
	// build a list
	for _, location := range storeLocations.list {
		loc, err := url.Parse(location)
		if err != nil {
			die(fmt.Errorf("Unable to parse store location %s : %s", location, err))
		}
		var s desync.Store
		switch loc.Scheme {
		case "ssh":
			r, err := desync.NewRemoteSSHStore(loc, n)
			if err != nil {
				die(err)
			}
			defer r.Close()
			s = r
		case "http", "https":
			s, err = desync.NewRemoteHTTPStore(loc)
			if err != nil {
				die(err)
			}
		case "":
			s, err = desync.NewLocalStore(loc.Path)
			if err != nil {
				die(err)
			}
		default:
			die(fmt.Errorf("Unsupported store access scheme %s", loc.Scheme))
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
			die(err)
		}
		cache.UpdateTimes = true
		s = desync.NewCache(s, cache)
	}

	var (
		wg   sync.WaitGroup
		in   = make(chan desync.ChunkID)
		mu   sync.Mutex
		errs []error
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
		cancel()
	}

	// Start the workers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for id := range in {
				if _, err := s.GetChunk(id); err != nil {
					recordError(err)
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop on any errors
	for id := range ids {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break
		default:
		}
		in <- id
	}
	close(in)
	wg.Wait()
}
