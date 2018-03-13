package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/folbricht/desync"
)

const mountIdxUsage = `desync mount-index [options] <index> <mountpoint>

FUSE mount of the blob in the index file. It makes the (single) file in
the index available for read access. Use 'extract' if the goal is to
assemble the whole blob locally as that is more efficient.
`

func mountIdx(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		err            error
		storeLocations = new(multiArg)
		clientCert     string
		clientKey      string
	)
	flags := flag.NewFlagSet("mount-index", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, mountIdxUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.StringVar(&clientCert, "clientCert", "", "Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Client Key for TLS authentication")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	indexFile := flags.Arg(0)
	mountPoint := flags.Arg(1)
	mountFName := strings.TrimSuffix(filepath.Base(indexFile), filepath.Ext(indexFile))

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	// Parse the store locations, open the stores and add a cache is requested
	s, err := MultiStoreWithCache(n, cacheLocation, clientCert, clientKey, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	idx, err := readCaibxFile(indexFile)
	if err != nil {
		return err
	}

	// Chop up the file into chunks and store them in the target store
	return desync.MountIndex(ctx, idx, mountPoint, mountFName, s, n)
}
