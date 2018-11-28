// +build !windows

package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type untarOptions struct {
	cmdStoreOptions
	desync.UntarOptions
	stores    []string
	cache     string
	readIndex bool
}

func newUntarCommand(ctx context.Context) *cobra.Command {
	var opt untarOptions

	cmd := &cobra.Command{
		Use:   "untar <catar|index> <target>",
		Short: "Extract directory tree from a catar archive or index",
		Long: `Extracts a directory tree from a catar file or an index. Use '-' to read the
index from STDIN.`,
		Example: `  desync untar docs.catar /tmp/documents
  desync untar -s http://192.168.1.1/ -c /path/to/local docs.caidx /tmp/documents`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUntar(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s), used with -i")
	flags.StringVarP(&opt.cache, "cache", "c", "", "store to be used as cache")
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	flags.BoolVarP(&opt.readIndex, "index", "i", false, "read index file (caidx), not catar")
	flags.BoolVar(&opt.NoSameOwner, "no-same-owner", false, "extract files as current user")
	flags.BoolVar(&opt.NoSamePermissions, "no-same-permissions", false, "use current user's umask instead of what is in the archive")
	return cmd
}

func runUntar(ctx context.Context, opt untarOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	if opt.readIndex && len(opt.stores) == 0 {
		return errors.New("-i requires at least one store (-s <location>)")
	}

	input := args[0]
	targetDir := args[1]

	// If we got a catar file unpack that and exit
	if !opt.readIndex {
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		var r io.Reader = f
		pb := NewProgressBar("Unpacking ")
		if pb != nil {
			// Get the file size to initialize the progress bar
			info, err := f.Stat()
			if err != nil {
				return err
			}
			pb.Start()
			defer pb.Finish()
			pb.SetTotal(int(info.Size()))
			r = io.TeeReader(f, pb)
		}
		return desync.UnTar(ctx, r, targetDir, opt.UntarOptions)
	}

	s, err := MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Apparently the input must be an index, read it whole
	index, err := readCaibxFile(input, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	return desync.UnTarIndex(ctx, targetDir, index, s, opt.n, opt.UntarOptions, NewProgressBar("Unpacking "))
}
