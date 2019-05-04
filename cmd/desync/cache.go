package main

import (
	"context"
	"errors"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type cacheOptions struct {
	cmdStoreOptions
	stores []string
	cache  string
}

func newCacheCommand(ctx context.Context) *cobra.Command {
	var opt cacheOptions

	cmd := &cobra.Command{
		Use:   "cache <index> [<index>...]",
		Short: "Read indexes and copy the referenced chunks",
		Long: `Read chunk IDs from caibx or caidx files from one or more stores without
writing to disk. Can be used (with -c) to populate a store with desired chunks
either to be used as cache, or to populate a store with chunks referenced in an
index file. Use '-' to read (a single) index from STDIN.`,
		Example: `  desync cache -s http://192.168.1.1/ -c /path/to/local file.caibx`,
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCache(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s)")
	flags.StringVarP(&opt.cache, "cache", "c", "", "target store")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runCache(ctx context.Context, opt cacheOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if len(opt.stores) == 0 {
		return errors.New("no source store provided")
	}
	if opt.cache == "" {
		return errors.New("no target cache store provided")
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	idm := make(map[desync.ChunkID]struct{})
	for _, name := range args {
		c, err := readCaibxFile(name, opt.cmdStoreOptions)
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

	s, err := multiStoreWithRouter(opt.cmdStoreOptions, opt.stores...)
	if err != nil {
		return err
	}
	defer s.Close()

	dst, err := WritableStore(opt.cache, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	defer dst.Close()

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar("")

	// Pull all the chunks, and load them into the cache in the process
	return desync.Copy(ctx, ids, s, dst, opt.n, pb)
}
