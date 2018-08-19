package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
)

const listUsage = `desync list-chunks <index>

Reads the index file and prints the list of chunk IDs in it. Use '-' to read
the index from STDIN.`

func list(ctx context.Context, args []string) error {
	var (
		clientCert string
		clientKey  string
	)

	flags := flag.NewFlagSet("list-chunks", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, listUsage)
		flags.PrintDefaults()
	}
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 1 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	// Parse the store locations, open the stores and add a cache is requested
	opts := storeOptions{
		clientCert: clientCert,
		clientKey:  clientKey,
	}

	// Read the input
	c, err := readCaibxFile(flags.Arg(0), opts)
	if err != nil {
		return err
	}
	// Write the list of chunk IDs to STDOUT
	for _, chunk := range c.Chunks {
		fmt.Println(chunk.ID)
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	return nil
}
