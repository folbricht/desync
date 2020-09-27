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
		Use:   "mtree <catar|index|dir>",
		Short: "Print the content of a catar, caidx or local directory in mtree format",
		Long: `Reads an archive (catar), index (caidx) or local directory and prints
the content in mtree format.

The input is either a catar archive, a caidx index file (with -i and -s), or
a local directory.
`,
		Example: `  desync mtree docs.catar
  desync mtree -s http://192.168.1.1/ -c /path/to/local -i docs.caidx
  desync mtree /path/to/dir`,
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
	mtreeFS, err := desync.NewMtreeFS(os.Stdout)
	if err != nil {
		return err
	}

	stat, err := os.Stat(input)
	if err != nil {
		return err
	}

	if opt.readIndex && stat.IsDir() {
		return errors.New("-i can't be used with input directory")
	}

	// Input is a directory, not an archive. So Tar it into an Untar stream
	// which then writes into an mtree writer.
	if stat.IsDir() {
		r, w := io.Pipe()
		inFS := desync.NewLocalFS(input, desync.LocalFSOptions{})

		// Run the tar bit in a goroutine, writing to the pipe
		var tarErr error
		go func() {
			tarErr = desync.Tar(ctx, w, inFS)
			w.Close()
		}()
		untarErr := desync.UnTar(ctx, r, mtreeFS)

		if tarErr != nil {
			return tarErr
		}
		return untarErr
	}

	// If we got a catar file unpack that and exit
	if !opt.readIndex {
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		var r io.Reader = f
		return desync.UnTar(ctx, r, mtreeFS)
	}

	s, err := MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	if err != nil {
		return err
	}
	defer s.Close()

	// The input must be an index, read it whole
	index, err := readCaibxFile(input, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	return desync.UnTarIndex(ctx, mtreeFS, index, s, opt.n, nil)
}
