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

	// An index doesn't have to be a local file, it can be read from STDIN or
	// a store, so it isn't looked for on the filesystem. A catar or a
	// directory always is one.
	if opt.readIndex {
		if stat, err := os.Stat(input); err == nil && stat.IsDir() {
			return errors.New("-i can't be used with input directory")
		}
		return mtreeIndex(ctx, opt, input)
	}

	stat, err := os.Stat(input)
	if err != nil {
		return err
	}

	// Nothing is written until the input is known to be readable, so a
	// failure doesn't leave a header on its own behind.
	mtreeFS, err := desync.NewMtreeFS(stdout)
	if err != nil {
		return err
	}

	// Input is a directory, not an archive. So Tar it into an Untar stream
	// which then writes into an mtree writer.
	if stat.IsDir() {
		r, w := io.Pipe()
		inFS := desync.NewLocalFS(input, desync.LocalFSOptions{})

		// Run the tar bit in a goroutine, writing to the pipe
		tarErr := make(chan error, 1)
		go func() {
			err := desync.Tar(ctx, w, inFS)
			w.CloseWithError(err)
			tarErr <- err
		}()
		untarErr := desync.UnTar(ctx, r, mtreeFS)

		// UnTar can give up before the stream ends, leaving Tar blocked on a
		// pipe nobody reads. Closing the read end lets it finish, so its
		// error can be collected rather than raced on.
		r.CloseWithError(untarErr)
		if err := <-tarErr; err != nil {
			return err
		}
		return untarErr
	}

	// What's left is a catar file, unpack that
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	return desync.UnTar(ctx, f, mtreeFS)
}

func mtreeIndex(ctx context.Context, opt mtreeOptions, input string) error {
	s, err := MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the index whole before writing anything
	index, err := readCaibxFile(input, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	mtreeFS, err := desync.NewMtreeFS(stdout)
	if err != nil {
		return err
	}

	return desync.UnTarIndex(ctx, mtreeFS, index, s, opt.n, desync.NullProgressBar{})
}
