package main

import (
	"context"
	"crypto/sha512"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	casync "github.com/folbricht/go-casync"
)

const usage = `desync [options] <caibx> <output>`

type multiArg struct {
	list []string
}

func (a *multiArg) Set(v string) error {
	a.list = append(a.list, v)
	return nil
}

func (a *multiArg) String() string { return "" }

func main() {
	var (
		cacheLocation  string
		n              int
		err            error
		storeLocations = new(multiArg)
		stores         []casync.Store
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Var(storeLocations, "s", "casync store location, can be multiples")
	flag.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flag.IntVar(&n, "n", 10, "number of goroutines")
	flag.Parse()

	if flag.NArg() < 2 {
		die(errors.New("Not enough arguments. See -h for help."))
	}
	if flag.NArg() > 2 {
		die(errors.New("Too many arguments. See -h for help."))
	}

	inFile := flag.Arg(0)
	outFile := flag.Arg(1)
	if inFile == outFile {
		die(errors.New("Input and output filenames match."))
	}

	// Checkout the store
	if len(storeLocations.list) == 0 {
		die(errors.New("No casync store provided. See -h for help."))
	}

	// Go through each stored passed in the command line, initialize them, and
	// build a list
	for _, location := range storeLocations.list {
		loc, err := url.Parse(location)
		if err != nil {
			die(fmt.Errorf("Unable to parse store location %s : %s", location, err))
		}
		var s casync.Store
		switch loc.Scheme {
		case "ssh":
			s, err = casync.NewRemoteSSHStore(loc, n)
			if err != nil {
				die(err)
			}
		case "http", "https":
			s, err = casync.NewRemoteHTTPStore(loc)
			if err != nil {
				die(err)
			}
		case "":
			s, err = casync.NewLocalStore(loc.Path)
			if err != nil {
				die(err)
			}
		default:
			die(fmt.Errorf("Unsupported store access scheme %s", loc.Scheme))
		}
		stores = append(stores, s)
	}

	// Combine all stores into one router
	var s casync.Store = casync.NewStoreRouter(stores...)

	// See if we want to use a local store as cache, if so, attach a cache to
	// the router
	if cacheLocation != "" {
		cache, err := casync.NewLocalStore(cacheLocation)
		if err != nil {
			die(err)
		}
		cache.UpdateTimes = true
		s = casync.NewCache(s, cache)
	}

	// Read the input
	c, err := readCaibxFile(inFile)
	if err != nil {
		die(err)
	}

	// Write the output
	if errs := writeOutput(outFile, c.Chunks, s, n); len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
}

func writeOutput(name string, chunks []casync.BlobIndexChunk, s casync.Store, n int) []error {
	// Prepare a tempfile that'll hold the output during processing. Close it, we
	// just need the name here since it'll be opened multiple times during write.
	// Also make sure it gets removed regardless of any errors below.
	tmpfile, err := ioutil.TempFile(filepath.Dir(name), ".desync")
	if err != nil {
		return []error{err}
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// Build the blob from the chunks, writing everything into the tempfile
	errs := assembleBlob(tmpfile.Name(), chunks, s, n)
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
func assembleBlob(name string, chunks []casync.BlobIndexChunk, s casync.Store, n int) []error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
		in   = make(chan casync.BlobIndexChunk)
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
				// Position the filehandle to the place where the chunk is meant to go
				// inside the file
				if _, err = f.Seek(int64(c.Start), io.SeekStart); err != nil {
					recordError(err)
					continue
				}
				// The the chunk is compressed. Decompress it into the output stream
				// while at the same time calculate the SHA512/256 so we can compare it.
				h := sha512.New512_256()
				mw := io.MultiWriter(h, f)
				if _, err = casync.DecompressInto(mw, b); err != nil {
					recordError(err)
					continue
				}
				sum, err := casync.ChunkIDFromSlice(h.Sum(nil))
				if err != nil {
					recordError(err)
					continue
				}
				if sum != c.ID {
					recordError(fmt.Errorf("unexpected sha256 %s for chunk id %s", sum, c.ID))
					continue
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop if there are any errors
	for _, c := range chunks {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break
		default:
		}
		in <- c
	}
	close(in)

	wg.Wait()

	return errs
}

func readCaibxFile(name string) (c casync.Caibx, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer f.Close()
	return casync.CaibxFromReader(f)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
