package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/folbricht/desync"
)

const extractUsage = `desync extract [options] <caibx> <output>

Read a caibx and build a blob reading chunks from one or more casync stores.
When using -k, the blob will be extracted in-place utilizing existing data and
the target file will not be deleted on error. This can be used to restart a
failed prior extraction without having to retrieve completed chunks again.
`

func extract(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		err            error
		storeLocations = new(multiArg)
		seedLocations  = new(multiArg)
		clientCert     string
		clientKey      string
		inPlace        bool
	)
	flags := flag.NewFlagSet("extract", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, extractUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.Var(seedLocations, "seed", "seed indexes, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.BoolVar(&inPlace, "k", false, "extract the file in place and keep it in case of error")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
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

	// Parse the store locations, open the stores and add a cache is requested
	var s desync.Store
	opts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}
	s, err = MultiStoreWithCache(opts, cacheLocation, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the target index
	idx, err := readCaibxFile(inFile)
	if err != nil {
		return err
	}

	// Build a list of seeds if any were given in the command line
	seeds, err := readSeeds(outFile, seedLocations.list)
	if err != nil {
		return err
	}

	if inPlace {
		return writeInplace(ctx, outFile, idx, s, seeds, n)
	}
	return writeWithTmpFile(ctx, outFile, idx, s, seeds, n)
}

func writeWithTmpFile(ctx context.Context, name string, idx desync.Index, s desync.Store, seeds []desync.Seed, n int) error {
	// Prepare a tempfile that'll hold the output during processing. Close it, we
	// just need the name here since it'll be opened multiple times during write.
	// Also make sure it gets removed regardless of any errors below.
	tmpfile, err := ioutil.TempFile(filepath.Dir(name), "."+filepath.Base(name))
	if err != nil {
		return err
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// Build the blob from the chunks, writing everything into the tempfile
	if err = writeInplace(ctx, tmpfile.Name(), idx, s, seeds, n); err != nil {
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

func writeInplace(ctx context.Context, name string, idx desync.Index, s desync.Store, seeds []desync.Seed, n int) error {
	// If this is a terminal, we want a progress bar
	p := NewProgressBar(len(idx.Chunks), "")
	p.Start()
	defer p.Stop()

	// Build the blob from the chunks, writing everything into given filename
	return desync.AssembleFile(ctx, name, idx, s, seeds, n, func() { p.Add(1) })
}

func readSeeds(dstFile string, locations []string) ([]desync.Seed, error) {
	var seeds []desync.Seed
	for _, srcIndexFile := range locations {
		srcIndex, err := readCaibxFile(srcIndexFile)
		if err != nil {
			return nil, err
		}
		srcFile := strings.TrimSuffix(srcIndexFile, ".caibx")

		seed, err := desync.NewIndexSeed(dstFile, desync.BlockSize, srcFile, srcIndex)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, seed)
	}
	return seeds, nil
}
