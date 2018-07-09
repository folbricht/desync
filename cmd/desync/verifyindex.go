package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const verifyIndexUsage = `desync verify-index [options] <caibx> <file>

Verifies an index file matches the content of a blob.
`

func verifyIndex(ctx context.Context, args []string) error {
	var (
		n int
	)
	flags := flag.NewFlagSet("verify-index", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, verifyIndexUsage)
		flags.PrintDefaults()
	}
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

	// Read the input
	idx, err := readCaibxFile(indexFile)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar("")

	// Chop up the file into chunks and store them in the target store
	return desync.VerifyIndex(ctx, dataFile, idx, n, pb)
}
