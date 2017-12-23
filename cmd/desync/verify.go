package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const verifyUsage = `desync verify -s <store> [-rn]

Reads all chunks in a local store and verifies their integrity. If -r is used,
invalid chunks are deleted from the store.`

func verify(args []string) error {
	var (
		repair        bool
		n             int
		err           error
		storeLocation string
	)
	flags := flag.NewFlagSet("verify", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, verifyUsage)
		flags.PrintDefaults()
	}
	flags.StringVar(&storeLocation, "s", "", "local store directory")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.BoolVar(&repair, "r", false, "Remove any invalid chunks")
	flags.Parse(args)

	if flags.NArg() > 0 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if storeLocation == "" {
		return errors.New("No store provided.")
	}

	s, err := desync.NewLocalStore(storeLocation)
	if err != nil {
		return err
	}
	return s.Verify(n, repair)
}
