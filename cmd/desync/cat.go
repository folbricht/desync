package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type catOptions struct {
	cmdStoreOptions
	stores         []string
	cache          string
	offset, length int
}

func newCatCommand(ctx context.Context) *cobra.Command {
	var opt catOptions

	cmd := &cobra.Command{
		Use:   "cat <index> [<output>]",
		Short: "Stream a blob to stdout or a file-like object",
		Long: `Stream a blob to stdout or a file-like object, optionally seeking and limiting
the read length.

Unlike extract, this supports output to FIFOs, named pipes, and other
non-seekable destinations.

This is inherently slower than extract as while multiple chunks can be
retrieved concurrently, writing to stdout cannot be parallelized.

Use '-' to read the index from STDIN.`,
		Example: `  desync cat -s http://192.168.1.1/ file.caibx | grep something`,
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCat(ctx, opt, args)
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
	flags.IntVarP(&opt.offset, "offset", "o", 0, "offset in bytes to seek to before reading")
	flags.IntVarP(&opt.length, "length", "l", 0, "number of bytes to read")
	return cmd
}

func runCat(ctx context.Context, opt catOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}

	var (
		outFile io.Writer
		err     error
	)
	if len(args) == 2 {
		outFileName := args[1]
		outFile, err = os.Create(outFileName)
		if err != nil {
			return err
		}
	} else {
		outFile = stdout
	}

	inFile := args[0]

	// Checkout the store
	if len(opt.stores) == 0 {
		return errors.New("no store provided")
	}

	// Parse the store locations, open the stores and add a cache is requested
	s, err := MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	c, err := readCaibxFile(inFile, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	// Write the output
	readSeeker := desync.NewIndexReadSeeker(c, s)
	if _, err = readSeeker.Seek(int64(opt.offset), io.SeekStart); err != nil {
		return err
	}

	if opt.length > 0 {
		_, err = io.CopyN(outFile, readSeeker, int64(opt.length))
	} else {
		_, err = io.Copy(outFile, readSeeker)
	}
	return err
}
