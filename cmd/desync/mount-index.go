// +build !windows

package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type mountIndexOptions struct {
	cmdStoreOptions
	stores     []string
	cache      string
	seeds      []string
	seedDirs   []string
	inPlace    bool
	printStats bool
}

func newMountIndexCommand(ctx context.Context) *cobra.Command {
	var opt mountIndexOptions

	cmd := &cobra.Command{
		Use:   "mount-index <index> <mountpoint>",
		Short: "FUSE mount an index file",
		Long: `FUSE mount of the blob in the index file. It makes the (single) file in
the index available for read access. Use 'extract' if the goal is to
assemble the whole blob locally as that is more efficient. Use '-' to read
the index from STDIN.`,
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
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	return cmd
}

func runMountIndex(ctx context.Context, opt mountIndexOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}

	indexFile := args[0]
	mountPoint := args[1]
	mountFName := strings.TrimSuffix(filepath.Base(indexFile), filepath.Ext(indexFile))

	// Checkout the store
	if len(opt.stores) == 0 {
		return errors.New("no store provided")
	}

	// Parse the store locations, open the stores and add a cache if requested
	s, err := MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	if err != nil {
		return err
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
