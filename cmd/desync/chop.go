package main

import (
	"context"
	"crypto/sha512"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/folbricht/desync"
)

const chopUsage = `desync chop [options] <caibx> <file>

Reads the index file and extracts all referenced chunks from the file into a local store.`

func chop(ctx context.Context, args []string) error {
	var (
		storeLocation string
		n             int
	)
	flags := flag.NewFlagSet("chop", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, chopUsage)
		flags.PrintDefaults()
	}
	flags.StringVar(&storeLocation, "s", "", "Local casync store location")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}
	indexFile := flags.Arg(0)
	dataFile := flags.Arg(1)

	// Open the target store
	s, err := desync.NewLocalStore(storeLocation)
	if err != nil {
		return err
	}

	// Read the input
	c, err := readCaibxFile(indexFile)
	if err != nil {
		return err
	}

	// Chop up the file into chunks and store them in the target store
	if errs := chopFile(ctx, dataFile, c.Chunks, s, n); len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
	return nil
}

func chopFile(ctx context.Context, name string, chunks []desync.IndexChunk, s desync.LocalStore, n int) []error {
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

	// Start the workers, each having its own filehandle to read concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		f, err := os.Open(name)
		if err != nil {
			return []error{fmt.Errorf("unable to open file %s, %s", name, err)}
		}
		defer f.Close()
		go func() {
			for c := range in {
				// Skip this chunk if the store already has it
				if s.HasChunk(c.ID) {
					continue
				}

				// Position the filehandle to the place where the chunk is meant to come
				// from within the file
				if _, err = f.Seek(int64(c.Start), io.SeekStart); err != nil {
					recordError(err)
					continue
				}

				// Read the whole (uncompressed) chunk into memory
				b := make([]byte, c.Size)
				if _, err = io.ReadFull(f, b); err != nil {
					recordError(err)
					continue
				}

				// Calculate this chunks checksum and compare to what it's supposed to be
				// according to the index
				sum := sha512.Sum512_256(b)
				if sum != c.ID {
					recordError(fmt.Errorf("chunk %s checksum does not match", c.ID))
					continue
				}

				// Compress the chunk
				cb, err := desync.Compress(b)
				if err != nil {
					recordError(err)
					continue
				}

				// And store it
				if err = s.StoreChunk(c.ID, cb); err != nil {
					recordError(err)
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
