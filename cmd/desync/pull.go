package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const pullUsage = `desync pull - - - <store>

Serves up chunks (read-only) from a local store using the casync protocol
via Stdin/Stdout. Functions as a drop-in replacement for casync on remote
stores accessed with SSH. See CASYNC_REMOTE_PATH environment variable.`

func pull(args []string) {
	flags := flag.NewFlagSet("pull", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, pullUsage)
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() != 4 {
		die(errors.New("Needs 4 arguments. See -h for help."))
	}

	storeLocation := flags.Arg(3)

	// Open the local store to serve chunks from
	s, err := desync.NewLocalStore(storeLocation)
	if err != nil {
		die(err)
	}

	// Start the server
	if err := desync.NewProtocolServer(os.Stdin, os.Stdout, s).Serve(); err != nil {
		die(err)
	}
}
