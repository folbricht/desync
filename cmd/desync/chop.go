package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

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
	if errs := desync.ChopFile(ctx, dataFile, c.Chunks, s, n); len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
	return nil
}
