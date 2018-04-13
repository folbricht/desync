// +build !windows

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const untarUsage = `desync untar <catar|caidx> <target>

Extracts a directory tree from a catar file or an index file.`

func untar(ctx context.Context, args []string) error {
	var (
		readIndex      bool
		n              int
		storeLocations = new(multiArg)
		cacheLocation  string
		clientCert     string
		clientKey      string
		opts           desync.UntarOptions
	)
	flags := flag.NewFlagSet("untar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, untarUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&readIndex, "i", false, "Read index file (caidx), not catar")
	flags.Var(storeLocations, "s", "casync store location, can be multiples (with -i)")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache (with -i)")
	flags.IntVar(&n, "n", 10, "number of goroutines (with -i)")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.BoolVar(&opts.NoSameOwner, "no-same-owner", false, "extract files as current user")
	flags.BoolVar(&opts.NoSamePermissions, "no-same-permissions", false, "use current user's umask instead of what is in the archive")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}
	if readIndex && len(storeLocations.list) == 0 {
		return errors.New("-i requires at least one store (-s <location>)")
	}
	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	input := flags.Arg(0)
	targetDir := flags.Arg(1)

	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()

	// If we got a catar file unpack that and exit
	if !readIndex {
		return desync.UnTar(ctx, f, targetDir, opts)
	}

	// Apparently the input must be an index, read it whole
	index, err := desync.IndexFromReader(f)
	if err != nil {
		return err
	}

	// Parse the store locations, open the stores and add a cache is requested
	s, err := MultiStoreWithCache(n, cacheLocation, clientCert, clientKey, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	return desync.UnTarIndex(ctx, targetDir, index, s, n, opts)
}
