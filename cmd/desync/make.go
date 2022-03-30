package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type makeOptions struct {
	cmdStoreOptions
	store      string
	chunkSize  string
	printStats bool
}

func newMakeCommand(ctx context.Context) *cobra.Command {
	var opt makeOptions

	cmd := &cobra.Command{
		Use:   "make <index> <file>",
		Short: "Chunk input file and create index",
		Long: `Creates chunks from the input file and builds an index. If a chunk store is
provided with -s, such as a local directory or S3 store, it splits the input
file according to the index and stores the chunks. Use '-' to write the index
to STDOUT.`,
		Example: `  desync make -s /path/to/local file.caibx largefile.bin`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMake(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store")
	flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in kb")
	flags.BoolVarP(&opt.printStats, "print-stats", "", false, "show chunking statistics")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runMake(ctx context.Context, opt makeOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}

	min, avg, max, err := parseChunkSizeParam(opt.chunkSize)
	if err != nil {
		return err
	}

	indexFile := args[0]
	dataFile := args[1]

	// Open the target store if one was given
	var s desync.WriteStore
	if opt.store != "" {
		s, err = WritableStore(opt.store, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		defer s.Close()
	}

	// Split up the file and create and index from it
	pb := desync.NewProgressBar("Chunking ")
	index, stats, err := desync.IndexFromFile(ctx, dataFile, opt.n, min, avg, max, pb)
	if err != nil {
		return err
	}

	// Chop up the file into chunks and store them in the target store if a store was given
	if s != nil {
		pb := desync.NewProgressBar("Storing ")
		if err := desync.ChopFile(ctx, dataFile, index.Chunks, s, opt.n, pb); err != nil {
			return err
		}
	}
	if opt.printStats {
		return printJSON(stderr, stats) // write to stderr since stdout could be used for index data
	}
	return storeCaibxFile(index, indexFile, opt.cmdStoreOptions)
}

func parseChunkSizeParam(s string) (min, avg, max uint64, err error) {
	sizes := strings.Split(s, ":")
	if len(sizes) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid chunk size '%s'", s)
	}
	num, err := strconv.Atoi(sizes[0])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "min chunk size")
	}
	min = uint64(num) * 1024
	num, err = strconv.Atoi(sizes[1])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "avg chunk size")
	}
	avg = uint64(num) * 1024
	num, err = strconv.Atoi(sizes[2])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "max chunk size")
	}
	max = uint64(num) * 1024
	return
}
