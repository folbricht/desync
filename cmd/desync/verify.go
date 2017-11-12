package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	casync "github.com/folbricht/go-casync"
)

const verifyUsage = `desync verify -s <store> [-rn]

Reads all chunks in a local store and verifies their integrity. If -r is used,
invalid chunks are deleted from the store.`

func verify(args []string) {
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
		die(errors.New("Too many arguments. See -h for help."))
	}

	if storeLocation == "" {
		die(errors.New("No store provided."))
	}

	s, err := casync.NewLocalStore(storeLocation)
	if err != nil {
		die(err)
	}
	if err := s.Verify(n, repair); err != nil {
		die(err)
	}
}
