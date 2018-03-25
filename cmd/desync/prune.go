package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/folbricht/desync"
)

const pruneUsage = `desync prune [options] <index> [<index>..]

Read chunk IDs in from index files and delete any chunks from a local (or s3)
store that are not referenced in the index files.`

func prune(ctx context.Context, args []string) error {
	var (
		storeLocation string
		accepted      bool
	)
	flags := flag.NewFlagSet("prune", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, pruneUsage)
		flags.PrintDefaults()
	}

	flags.StringVar(&storeLocation, "s", "", "local store directory")
	flags.BoolVar(&accepted, "y", false, "do not ask for confirmation")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}

	if storeLocation == "" {
		return errors.New("No store provided.")
	}

	// Open the target store
	s, err := WritableStore(1, storeLocation)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input files and merge all chunk IDs in a map to de-dup them
	ids := make(map[desync.ChunkID]struct{})
	for _, name := range flags.Args() {
		c, err := readCaibxFile(name)
		if err != nil {
			return err
		}
		for _, c := range c.Chunks {
			ids[c.ID] = struct{}{}
		}
	}

	// If the -y option wasn't provided, ask the user to confirm
	if !accepted {
		fmt.Printf("Warning: The provided index files reference %d unique chunks. Are you sure\nyou want to delete all other chunks from '%s'?\n", len(ids), s)
	acceptance:
		for {
			var a string
			fmt.Printf("[y/N]: ")
			fmt.Fscanln(os.Stdin, &a)
			switch a {
			case "y", "Y":
				break acceptance
			case "n", "N", "":
				return nil
			}
		}
	}

	return s.Prune(ctx, ids)
}
