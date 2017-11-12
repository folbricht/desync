package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

const listUsage = `desync list-chunks <caibx>

Reads the index file from disk and prints the list of chunk IDs in it.`

func list(args []string) {
	flags := flag.NewFlagSet("list-chunks", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, listUsage)
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() < 1 {
		die(errors.New("Not enough arguments. See -h for help."))
	}
	if flags.NArg() > 1 {
		die(errors.New("Too many arguments. See -h for help."))
	}

	// Read the input
	c, err := readCaibxFile(flags.Arg(0))
	if err != nil {
		die(err)
	}
	// Write the list of chunk IDs to STDOUT
	for _, chunk := range c.Chunks {
		fmt.Println(chunk.ID)
	}
}
