package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
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
		clientCert     string
		clientKey      string
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

	// Write the output
	return writeOutput(ctx, outFile, idx, s, n)
}

func writeOutput(ctx context.Context, name string, idx desync.Index, s desync.Store, n int) error {
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
		p := NewProgressBar(int(os.Stderr.Fd()), len(idx.Chunks))
		p.Start()
		defer p.Stop()
		progress = func() { p.Add(1) }
	}

	// Build the blob from the chunks, writing everything into the tempfile
	if err = desync.AssembleFile(ctx, tmpfile.Name(), idx, s, n, progress); err != nil {
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
