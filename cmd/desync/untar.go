package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const untarUsage = `desync untar <catar> <target>

Extracts a directory tree from a catar file.`

func untar(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("untar", flag.ExitOnError)
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
	targetDir := flags.Arg(1)

	f, err := os.Open(catarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	return desync.UnTar(ctx, f, targetDir)
}
