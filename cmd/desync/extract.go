package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/folbricht/desync"
	"golang.org/x/crypto/ssh/terminal"
)

const extractUsage = `desync extract [options] <caibx> <output>

Read a caibx and build a blob reading chunks from one or more casync stores.`

func extract(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		err            error
		storeLocations = new(multiArg)
		stores         []desync.Store
	)
	flags := flag.NewFlagSet("extract", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, extractUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	inFile := flags.Arg(0)
	outFile := flags.Arg(1)
	if inFile == outFile {
		return errors.New("Input and output filenames match.")
	}

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
	return writeOutput(ctx, outFile, c.Chunks, s, n)
}

func writeOutput(ctx context.Context, name string, chunks []desync.IndexChunk, s desync.Store, n int) error {
	// Prepare a tempfile that'll hold the output during processing. Close it, we
	// just need the name here since it'll be opened multiple times during write.
	// Also make sure it gets removed regardless of any errors below.
	tmpfile, err := ioutil.TempFile(filepath.Dir(name), "."+filepath.Base(name))
	if err != nil {
		return err
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// If this is a terminal, we want a progress bar
	var progress func()
	if terminal.IsTerminal(int(os.Stderr.Fd())) {
		p := NewProgressBar(int(os.Stderr.Fd()), len(chunks))
		p.Start()
		defer p.Stop()
		progress = func() { p.Add(1) }
	}

	// Build the blob from the chunks, writing everything into the tempfile
	if err = desync.AssembleFile(ctx, tmpfile.Name(), chunks, s, n, progress); err != nil {
		return err
	}

	// Rename the tempfile to the output file
	if err := os.Rename(tmpfile.Name(), name); err != nil {
		return err
	}

	// FIXME Unfortunately, tempfiles are created with 0600 perms and there doesn't
	// appear a way to influence that, short of writing another function that
	// generates a tempfile name. Set 0644 perms here after rename (ignoring umask)
	return os.Chmod(name, 0644)
}
