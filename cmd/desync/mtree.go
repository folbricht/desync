package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type mtreeOptions struct {
	cmdStoreOptions
	stores    []string
	cache     string
	readIndex bool
}

func newMtreeCommand(ctx context.Context) *cobra.Command {
	var opt mtreeOptions

	cmd := &cobra.Command{
		Use:   "mtree <catar|index>",
		Short: "Print the content of a catar or caidx in mtree format",
		Long: `Reads an archive (catar) or index (caidx) and prints the content in mtree
format.

The input is either a catar archive, or a caidx index file (with -i and -s).
`,
		Example: `  desync mtree docs.catar
  desync mtree -s http://192.168.1.1/ -c /path/to/local -i docs.caidx`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMtree(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s), used with -i")
	flags.StringVarP(&opt.cache, "cache", "c", "", "store to be used as cache")
	flags.BoolVarP(&opt.readIndex, "index", "i", false, "read index file (caidx), not catar")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runMtree(ctx context.Context, opt mtreeOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if opt.readIndex && len(opt.stores) == 0 {
		return errors.New("-i requires at least one store (-s <location>)")
	}

	input := args[0]
	fs, err := desync.NewMtreeFS(os.Stdout)
	if err != nil {
		return err
	}

	// If we got a catar file unpack that and exit
	if !opt.readIndex {
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		var r io.Reader = f
		return desync.UnTar(ctx, r, fs)
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

	return desync.UnTarIndex(ctx, fs, index, s, opt.n, nil)
}
