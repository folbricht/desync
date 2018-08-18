package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const chopUsage = `desync chop [options] <index> <file>

Reads the index and extracts all referenced chunks from the file into a store,
local or remote. Use '-' to read the index from STDIN.`

func chop(ctx context.Context, args []string) error {
	var (
		storeLocation string
		n             int
		clientCert    string
		clientKey     string
	)
	flags := flag.NewFlagSet("chop", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, chopUsage)
		flags.PrintDefaults()
	}
	flags.StringVar(&storeLocation, "s", "", "Local casync store location")
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

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	indexFile := flags.Arg(0)
	dataFile := flags.Arg(1)

	// Parse the store locations, open the stores and add a cache is requested
	opts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}

	// Open the target store
	s, err := WritableStore(storeLocation, opts)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	c, err := readCaibxFile(indexFile, opts)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar(len(c.Chunks), "")
	// Chop up the file into chunks and store them in the target store
	return desync.ChopFile(ctx, dataFile, c.Chunks, s, n, pb)
}
