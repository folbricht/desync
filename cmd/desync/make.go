package main

import (
	"context"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type makeOptions struct {
	cmdStoreOptions
	cmdChunkerOptions
	store      string
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
		Example: `  desync make -s /path/to/local file.caibx largefile.bin
  desync make -m 8:32:128 - largefile.bin > file.caibx`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMake(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store")
	flags.BoolVarP(&opt.printStats, "print-stats", "", false, "print chunking statistics to stderr when done")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	addChunkerOptions(&opt.cmdChunkerOptions, flags)
	return cmd
}

func runMake(ctx context.Context, opt makeOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}

	chunkerParams, err := opt.cmdChunkerOptions.ToChunkerParams()
	if err != nil {
		return err
	}

	indexFile := args[0]
	dataFile := args[1]

	if err := validateIndexLocation(indexFile); err != nil {
		return err
	}

	workers, err := opt.storeWorkers(opt.store)
	if err != nil {
		return err
	}
	opt.workers = workers

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
	index, stats, err := desync.IndexFromFile(ctx, dataFile, opt.cpuWorkers(), opt.cmdChunkerOptions.name, chunkerParams, pb)
	if err != nil {
		return err
	}

	// Chop up the file into chunks and store them in the target store if a store was given
	if s != nil {
		pb := desync.NewProgressBar("Storing ")
		if err := desync.ChopFile(ctx, dataFile, index.Chunks, s, workers, pb); err != nil {
			return err
		}
	}
	if opt.printStats {
		_ = printJSON(stderr, stats) // write to stderr since stdout could be used for index data
	}
	return storeCaibxFile(index, indexFile, opt.cmdStoreOptions)
}
