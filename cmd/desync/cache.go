package main

import (
	"context"
	"errors"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type cacheOptions struct {
	cmdStoreOptions
	stores        []string
	cache         string
	ignoreIndexes []string
	ignoreChunks  []string
}

func newCacheCommand(ctx context.Context) *cobra.Command {
	var opt cacheOptions

	cmd := &cobra.Command{
		Use:   "cache <index> [<index>...]",
		Short: "Read indexes and copy the referenced chunks",
		Long: `Read chunk IDs from caibx or caidx files from one or more stores without
writing to disk. Can be used (with -c) to populate a store with desired chunks
either to be used as cache, or to populate a store with chunks referenced in an
index file. Use '-' to read (a single) index from STDIN.

To exclude chunks that are known to exist in the target store already, use
--ignore <index> which will skip any chunks from the given index. The same can
be achieved by providing the chunks in their ASCII representation in a text
file with --ignore-chunks <file>.`,
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
	flags.StringSliceVarP(&opt.ignoreIndexes, "ignore", "", nil, "index(s) to ignore chunks from")
	flags.StringSliceVarP(&opt.ignoreChunks, "ignore-chunks", "", nil, "ignore chunks from text file")
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
	// If requested, skip/ignore all chunks that are referenced in other indexes or text files
	if len(opt.ignoreIndexes) > 0 || len(opt.ignoreChunks) > 0 {
		// Remove chunks referenced in indexes
		for _, f := range opt.ignoreIndexes {
			i, err := readCaibxFile(f, opt.cmdStoreOptions)
			if err != nil {
				return err
			}
			for _, c := range i.Chunks {
				delete(idm, c.ID)
			}
		}

		// Remove chunks referenced in ASCII text files
		for _, f := range opt.ignoreChunks {
			ids, err := readChunkIDFile(f)
			if err != nil {
				return err
			}
			for _, id := range ids {
				delete(idm, id)
			}
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
	pb := desync.NewProgressBar("")

	// Pull all the chunks, and load them into the cache in the process
	return desync.Copy(ctx, ids, s, dst, opt.n, pb)
}
