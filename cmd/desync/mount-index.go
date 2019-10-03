// +build !windows

package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type mountIndexOptions struct {
	cmdStoreOptions
	stores    []string
	cache     string
	storeFile string
}

func newMountIndexCommand(ctx context.Context) *cobra.Command {
	var opt mountIndexOptions

	cmd := &cobra.Command{
		Use:   "mount-index <index> <mountpoint>",
		Short: "FUSE mount an index file",
		Long: `FUSE mount of the blob in the index file. It makes the (single) file in
the index available for read access. Use 'extract' if the goal is to
assemble the whole blob locally as that is more efficient. Use '-' to read
the index from STDIN.

This command supports the --store-file option which can be used to define the stores
and caches in a JSON file. The config can then be reloaded by sending a SIGHUP without
needing to restart the server. This can be done under load as well.
`,
		Example: `  desync mount-index -s http://192.168.1.1/ file.caibx /mnt/blob`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMountIndex(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s)")
	flags.StringVarP(&opt.cache, "cache", "c", "", "store to be used as cache")
	flags.StringVar(&opt.storeFile, "store-file", "", "read store arguments from a file, supports reload on SIGHUP")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runMountIndex(ctx context.Context, opt mountIndexOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}

	indexFile := args[0]
	mountPoint := args[1]
	mountFName := strings.TrimSuffix(filepath.Base(indexFile), filepath.Ext(indexFile))

	// Parse the store locations, open the stores and add a cache if requested
	s, err := mountIndexStore(opt)
	if err != nil {
		return err
	}

	// When a store file is used, it's possible to reload the store setup from it
	// on the fly. Wrap the store into a SwapStore and start a handler for SIGHUP,
	// reloading the store config from file.
	if opt.storeFile != "" {
		s = desync.NewSwapStore(s)

		go func() {
			for range sighup {
				log.Println("requested config reload")
				newStore, err := mountIndexStore(opt)
				if err != nil {
					fmt.Fprintln(stderr, "failed to reload configuration:", err)
					continue
				}
				if store, ok := s.(*desync.SwapStore); ok {
					log.Println("starting config reload")
					if err := store.Swap(newStore); err != nil {
						fmt.Fprintln(stderr, "failed to reload configuration:", err)
					}
					log.Println("done config reload")
				}
			}
		}()
	}

	defer s.Close()

	// Read the index
	idx, err := readCaibxFile(indexFile, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	// Mount it
	return desync.MountIndex(ctx, idx, mountPoint, mountFName, s, opt.n)
}

// Reads the store-related command line options and returns the appropriate store.
func mountIndexStore(opt mountIndexOptions) (desync.Store, error) {
	stores := opt.stores
	cache := opt.cache

	var err error
	if opt.storeFile != "" {
		if len(stores) != 0 {
			return nil, errors.New("--store and --store-file can't be used together")
		}
		if cache != "" {
			return nil, errors.New("--cache and --store-file can't be used together")
		}
		stores, cache, err = readStoreFile(opt.storeFile)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read store-file '%s'", err)
		}
	}

	// Got to have at least one upstream store
	if len(stores) == 0 {
		return nil, errors.New("no store provided")
	}
	return MultiStoreWithCache(opt.cmdStoreOptions, cache, stores...)
}
