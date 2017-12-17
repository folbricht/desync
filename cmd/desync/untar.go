package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

const untarUsage = `desync untar <catar> <target>

Extracts a directory tree from a catar file.`

func untar(args []string) {
	flags := flag.NewFlagSet("untar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, untarUsage)
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() < 2 {
		die(errors.New("Not enough arguments. See -h for help."))
	}
	if flags.NArg() > 2 {
		die(errors.New("Too many arguments. See -h for help."))
	}

	// Read the input
	catarFile := flags.Arg(0)
	// targetDir := flags.Arg(1)

	f, err := os.Open(catarFile)
	if err != nil {
		die(err)
	}
	defer f.Close()

	die(errors.New("not yet implemented"))
}
