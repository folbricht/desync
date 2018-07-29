package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/folbricht/desync"
	"github.com/folbricht/tempfile"
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

	// Read the input
	idx, err := readCaibxFile(inFile)
	if err != nil {
		return err
	}

	if inPlace {
		return writeInplace(ctx, outFile, idx, s, n)
	}
	return writeWithTmpFile(ctx, outFile, idx, s, n)
}

func writeWithTmpFile(ctx context.Context, name string, idx desync.Index, s desync.Store, n int) error {
	// Prepare a tempfile that'll hold the output during processing. Close it, we
	// just need the name here since it'll be opened multiple times during write.
	// Also make sure it gets removed regardless of any errors below.
	tmp, err := tempfile.NewMode(filepath.Dir(name), "."+filepath.Base(name), 0644)
	if err != nil {
		return err
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	// Build the blob from the chunks, writing everything into the tempfile
	if err = writeInplace(ctx, tmp.Name(), idx, s, n); err != nil {
		return err
	}

	// Rename the tempfile to the output file
	return os.Rename(tmp.Name(), name)
}

func writeInplace(ctx context.Context, name string, idx desync.Index, s desync.Store, n int) error {
	// If this is a terminal, we want a progress bar
	p := NewProgressBar(len(idx.Chunks), "")
	p.Start()
	defer p.Stop()

	// Build the blob from the chunks, writing everything into given filename
	return desync.AssembleFile(ctx, name, idx, s, n, func() { p.Add(1) })
}
