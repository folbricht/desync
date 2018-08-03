package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const cacheUsage = `desync cache [options] <index> [<index>...]

Read chunk IDs from caibx or caidx files from one or more stores without
writing to disk. Can be used (with -c) to populate a store with desired chunks
either to be used as cache, or to populate a store with chunks referenced in an
index file.`

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

	flags.Var(storeLocations, "s", "source store location, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "target store to be used as cache")
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
		return errors.New("No source store provided. See -h for help.")
	}
	if cacheLocation == "" {
		return errors.New("No target cache store provided. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	// Parse the store locations, open the stores and add a cache is requested
	opts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	idm := make(map[desync.ChunkID]struct{})
	for _, name := range flags.Args() {
		c, err := readCaibxFile(name, opts)
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

	s, err := multiStore(opts, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	dst, err := WritableStore(cacheLocation, opts)
	if err != nil {
		return err
	}
	defer dst.Close()

	// If this is a terminal, we want a progress bar
	p := NewProgressBar(len(ids), "")
	p.Start()
	defer p.Stop()

	// Pull all the chunks, and load them into the cache in the process
	return desync.Copy(ctx, ids, s, dst, n, func() { p.Add(1) })
}
