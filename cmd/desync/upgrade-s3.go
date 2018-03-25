package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const upgradeS3Usage = `desync upgrade-s3 -s <s3-store>

Upgrades an S3 store using the deprecated layout (flat structure) to the new
layout which mirrors local stores. In the new format, each chunk is prefixed
with the 4 first characters of the checksum and prefixed with .cacnk`

func upgradeS3(ctx context.Context, args []string) error {
	var (
		storeLocation string
	)
	flags := flag.NewFlagSet("upgrade-s3", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, pruneUsage)
		flags.PrintDefaults()
	}

	flags.StringVar(&storeLocation, "s", "", "local store directory")
	flags.Parse(args)

	if flags.NArg() > 0 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if storeLocation == "" {
		return errors.New("No store provided.")
	}

	// Open the target store
	s, err := desync.NewS3Store(storeLocation)
	if err != nil {
		return err
	}
	defer s.Close()

	return s.Upgrade(ctx)
}
