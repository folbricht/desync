package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
	"golang.org/x/crypto/ssh/terminal"
)

const cacheUsage = `desync cache [options] <index> [<index>...]

Read chunk IDs in caibx files from one or more stores without creating a blob.
Can be used to pre-populate a local cache.`

func cache(ctx context.Context, args []string) error {
	var (
		cacheLocation  string
		n              int
		storeLocations = new(multiArg)
		clientCert     string
		clientKey      string
	)
	flags := flag.NewFlagSet("cache", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, cacheUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	idm := make(map[desync.ChunkID]struct{})
	for _, name := range flags.Args() {
		c, err := readCaibxFile(name)
		if err != nil {
			return err
		}
		for _, c := range c.Chunks {
			idm[c.ID] = struct{}{}
		}
	}

	// Now put the IDs into an array for further processing
	ids := make([]desync.ChunkID, 0, len(idm))
	for id := range idm {
		ids = append(ids, id)
	}

	// Parse the store locations, open the stores and add a cache is requested
	s, err := MultiStoreWithCache(n, cacheLocation, clientCert, clientKey, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	// If this is a terminal, we want a progress bar
	var progress func()
	if terminal.IsTerminal(int(os.Stderr.Fd())) {
		p := NewProgressBar(int(os.Stderr.Fd()), len(ids))
		p.Start()
		defer p.Stop()
		progress = func() { p.Add(1) }
	}

	// Pull all the chunks, and load them into the cache in the process
	return desync.Touch(ctx, ids, s, n, progress)
}
