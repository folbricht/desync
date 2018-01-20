package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/folbricht/desync"
)

const untarUsage = `desync untar <catar> <target>

Extracts a directory tree from a catar file or an index file.`

func untar(ctx context.Context, args []string) error {
	var (
		readIndex      bool
		n              int
		storeLocations = new(multiArg)
		stores         []desync.Store
		cacheLocation  string
	)
	flags := flag.NewFlagSet("untar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, untarUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&readIndex, "i", false, "Read index file (caidx), not catar")
	flags.Var(storeLocations, "s", "casync store location, can be multiples (with -i)")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache (with -i)")
	flags.IntVar(&n, "n", 10, "number of goroutines (with -i)")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	input := flags.Arg(0)
	targetDir := flags.Arg(1)

	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()

	// If we got a catar file unpack that and exit
	if !readIndex {
		return desync.UnTar(ctx, f, targetDir)
	}

	// Apparently the input must be an index, read it whole
	index, err := desync.IndexFromReader(f)
	if err != nil {
		return err
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

	return desync.UnTarIndex(ctx, targetDir, index, s, n)
}
