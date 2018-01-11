package main

import (
	"context"
	"crypto/sha512"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/folbricht/desync"
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
	if errs := writeOutput(ctx, outFile, c.Chunks, s, n); len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
	return nil
}

func writeOutput(ctx context.Context, name string, chunks []desync.IndexChunk, s desync.Store, n int) []error {
	// Prepare a tempfile that'll hold the output during processing. Close it, we
	// just need the name here since it'll be opened multiple times during write.
	// Also make sure it gets removed regardless of any errors below.
	tmpfile, err := ioutil.TempFile(filepath.Dir(name), "."+filepath.Base(name))
	if err != nil {
		return []error{err}
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// Build the blob from the chunks, writing everything into the tempfile
	errs := assembleBlob(ctx, tmpfile.Name(), chunks, s, n)
	if len(errs) != 0 {
		return errs
	}

	// Rename the tempfile to the output file
	if err := os.Rename(tmpfile.Name(), name); err != nil {
		return []error{err}
	}

	// FIXME Unfortunately, tempfiles are created with 0600 perms and there doesn't
	// appear a way to influence that, short of writing another function that
	// generates a tempfile name. Set 0644 perms here after rename (ignoring umask)
	if err := os.Chmod(name, 0644); err != nil {
		return []error{err}
	}
	return nil
}

// Opens n goroutines, creating one filehandle for the file "name" per goroutine
// and writes to the file simultaneously
func assembleBlob(ctx context.Context, name string, chunks []desync.IndexChunk, s desync.Store, n int) []error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
		in   = make(chan desync.IndexChunk)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
		cancel()
	}

	// Start the workers, each having its own filehandle to write concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return []error{fmt.Errorf("unable to open tempfile %s, %s", name, err)}
		}
		defer f.Close()
		go func() {
			for c := range in {
				// Pull the (compressed) chunk from the store
				b, err := s.GetChunk(c.ID)
				if err != nil {
					recordError(err)
					continue
				}
				// Since we know how big the chunk is supposed to be, pre-allocate a
				// slice to decompress into
				db := make([]byte, c.Size)
				// The the chunk is compressed. Decompress it here
				db, err = desync.Decompress(db, b)
				if err != nil {
					recordError(err)
					continue
				}
				// Verify the checksum of the chunk matches the ID
				sum := sha512.Sum512_256(db)
				if sum != c.ID {
					recordError(fmt.Errorf("unexpected sha512/256 %s for chunk id %s", sum, c.ID))
					continue
				}
				// Might as well verify the chunk size while we're at it
				if c.Size != uint64(len(db)) {
					recordError(fmt.Errorf("unexpected size for chunk %s", c.ID))
					continue
				}
				// Write the decompressed chunk into the file at the right position
				if _, err = f.WriteAt(db, int64(c.Start)); err != nil {
					recordError(err)
					continue
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop if there are any errors
loop:
	for _, c := range chunks {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		in <- c
	}
	close(in)

	wg.Wait()

	return errs
}
