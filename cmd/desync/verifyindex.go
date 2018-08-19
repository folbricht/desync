package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const verifyIndexUsage = `desync verify-index [options] <index> <file>

Verifies an index file matches the content of a blob. Use '-' to read the index
from STDIN.
`

func verifyIndex(ctx context.Context, args []string) error {
	var (
		n          int
		clientCert string
		clientKey  string
	)
	flags := flag.NewFlagSet("verify-index", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, verifyIndexUsage)
		flags.PrintDefaults()
	}
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}
	indexFile := flags.Arg(0)
	dataFile := flags.Arg(1)

	// Parse the store locations, open the stores and add a cache is requested
	opts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}

	// Read the input
	idx, err := readCaibxFile(indexFile, opts)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar("")

	// Chop up the file into chunks and store them in the target store
	return desync.VerifyIndex(ctx, dataFile, idx, n, pb)
}
