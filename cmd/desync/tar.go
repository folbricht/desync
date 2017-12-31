package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const tarUsage = `desync tar <catar> <source>

Encodes a directory tree into a catar archive.`

func tar(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("tar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, untarUsage)
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	catarFile := flags.Arg(0)
	sourceDir := flags.Arg(1)

	f, err := os.Create(catarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	return desync.Tar(ctx, f, sourceDir)
}
