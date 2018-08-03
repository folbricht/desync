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

Reads the index file and extracts all referenced chunks from the file
into a local or S3 store.`

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
	s, err := WritableStore(storeLocation, storeOptions{n: n})
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	c, err := readCaibxFile(indexFile)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar(len(c.Chunks), "")
	// Chop up the file into chunks and store them in the target store
	return desync.ChopFile(ctx, dataFile, c.Chunks, s, n, pb)
}
