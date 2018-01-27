package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/folbricht/desync"
	"io"
)

const catUsage = `desync cat [options] <caibx>

Stream a caibx to stdout, optionally seeking and limiting the read length.

This is inherently slower than extract as while multiple chunks can be retrieved
concurrently, writing to stdout cannot be parallelized.`

func cat(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		err            error
		storeLocations = new(multiArg)
		stores         []desync.Store
		offset         int
		length         int
		readIndex      bool
	)
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, catUsage)
		flags.PrintDefaults()
	}

	flags.BoolVar(&readIndex, "i", false, "Read index file (caidx), not catar, in 2-argument mode")
	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.IntVar(&offset, "o", 0, "offset in bytes to seek to before reading")
	flags.IntVar(&length, "l", 0, "number of bytes to read")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	var outFile io.Writer
	if flags.NArg() == 2 {
		outFileName := flags.Arg(1)
		outFile, err = os.Create(outFileName)
		if err != nil {
			return err
		}
	} else {
		outFile = os.Stdout
	}

	inFile := flags.Arg(0)
	//containedFile := flags.Arg(1)

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	// Go through each stored passed in the command line, initialize them, and
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

	// Read the input
	c, err := readCaibxFile(inFile)
	if err != nil {
		return err
	}

	// Write the output
	readSeeker := desync.NewIndexReadSeeker(c, s)
	readSeeker.Seek(int64(offset), io.SeekStart)

	if length > 0 {
		_, err = io.CopyN(outFile, &readSeeker, int64(length))
	} else {
		_, err = io.Copy(outFile, &readSeeker)
	}
	return err
}
