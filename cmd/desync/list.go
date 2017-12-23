package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
)

const listUsage = `desync list-chunks <caibx>

Reads the index file from disk and prints the list of chunk IDs in it.`

func list(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("list-chunks", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, listUsage)
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 1 {
		return errors.New("Too many arguments. See -h for help.")
	}

	// Read the input
	c, err := readCaibxFile(flags.Arg(0))
	if err != nil {
		return err
	}
	// Write the list of chunk IDs to STDOUT
	for _, chunk := range c.Chunks {
		fmt.Println(chunk.ID)
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break
		default:
		}
	}
	return nil
}
