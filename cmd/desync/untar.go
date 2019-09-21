// +build !windows

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type untarOptions struct {
	cmdStoreOptions
	desync.LocalFSOptions
	stores    []string
	cache     string
	readIndex bool
	outFormat string
}

func newUntarCommand(ctx context.Context) *cobra.Command {
	var opt untarOptions

	cmd := &cobra.Command{
		Use:   "untar <catar|index> <target>",
		Short: "Extract directory tree from a catar archive or index",
		Long: `Extracts a directory tree from a catar file or an index. Use '-' to read the
index from STDIN.

The input is either a catar archive, or a caidx index file (with -i and -s).

By default, the catar archive is extracted to local disk. Using --output-format=gnu-tar,
the output can be set to GNU tar, either an archive or STDOUT with '-'.
`,
		Example: `  desync untar docs.catar /tmp/documents
  desync untar -s http://192.168.1.1/ -c /path/to/local -i docs.caidx /tmp/documents`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUntar(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s), used with -i")
	flags.StringVarP(&opt.cache, "cache", "c", "", "store to be used as cache")
	flags.BoolVarP(&opt.readIndex, "index", "i", false, "read index file (caidx), not catar")
	flags.BoolVar(&opt.NoSameOwner, "no-same-owner", false, "extract files as current user")
	flags.BoolVar(&opt.NoSamePermissions, "no-same-permissions", false, "use current user's umask instead of what is in the archive")
	flags.StringVar(&opt.outFormat, "output-format", "disk", "output format, 'disk' or 'gnu-tar'")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runUntar(ctx context.Context, opt untarOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if opt.readIndex && len(opt.stores) == 0 {
		return errors.New("-i requires at least one store (-s <location>)")
	}

	input := args[0]
	target := args[1]

	// Prepare output
	var (
		fs  desync.FilesystemWriter
		err error
	)
	switch opt.outFormat {
	case "disk": // Local filesystem
		fs = desync.NewLocalFS(target, opt.LocalFSOptions)
	case "gnu-tar": // GNU tar, either file or STDOUT
		var w *os.File
		if target == "-" {
			w = os.Stdout
		} else {
			w, err = os.Create(target)
			if err != nil {
				return err
			}
			defer w.Close()
		}
		gtar := desync.NewTarWriter(w)
		defer gtar.Close()
		fs = gtar
	default:
		return fmt.Errorf("invalid output format '%s'", opt.outFormat)
	}

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

	return desync.UnTarIndex(ctx, fs, index, s, opt.n, NewProgressBar("Unpacking "))
}
