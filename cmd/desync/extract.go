package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/folbricht/desync"
	"github.com/folbricht/tempfile"
	"github.com/spf13/cobra"
)

type extractOptions struct {
	cmdStoreOptions
	stores     []string
	cache      string
	seeds      []string
	seedDirs   []string
	inPlace    bool
	printStats bool
}

func newExtractCommand(ctx context.Context) *cobra.Command {
	var opt extractOptions

	cmd := &cobra.Command{
		Use:   "extract <index> <output>",
		Short: "Read an index and build a blob from it",
		Long: `Reads an index and builds a blob reading chunks from one or more chunk stores.
When using -k, the blob will be extracted in-place utilizing existing data and
the target file will not be deleted on error. This can be used to restart a
failed prior extraction without having to retrieve completed chunks again.
Muptiple optional seed indexes can be given with -seed. The matching blob needs
to have the same name as the indexfile without the .caibx extension. If several
seed files and indexes are available, the -seed-dir option can be used to
automatically select call .caibx files in a directory as seeds. Use '-' to read
the index from STDIN.`,
		Example: `  desync extract -s http://192.168.1.1/ -c /path/to/local file.caibx largefile.bin
  desync extract -s /mnt/store -s /tmp/other/store file.tar.caibx file.tar
  desync extract -s /mnt/store --seed /mnt/v1.caibx v2.caibx v2.vmdk`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExtract(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s)")
	flags.StringSliceVar(&opt.seeds, "seed", nil, "seed indexes")
	flags.StringSliceVar(&opt.seedDirs, "seed-dir", nil, "directory with seed index files")
	flags.StringVarP(&opt.cache, "cache", "c", "", "store to be used as cache")
	flags.BoolVarP(&opt.inPlace, "in-place", "k", false, "extract the file in place and keep it in case of error")
	flags.BoolVarP(&opt.printStats, "print-stats", "", false, "print statistics")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runExtract(ctx context.Context, opt extractOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}

	inFile := args[0]
	outFile := args[1]
	if inFile == outFile {
		return errors.New("input and output filenames match")
	}

	// Checkout the store
	if len(opt.stores) == 0 {
		return errors.New("no store provided")
	}

	// Parse the store locations, open the stores and add a cache is requested
	var s desync.Store
	s, err := MultiStoreWithCache(opt.cmdStoreOptions, opt.cache, opt.stores...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	idx, err := readCaibxFile(inFile, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	// Build a list of seeds if any were given in the command line
	seeds, err := readSeeds(outFile, opt.seeds, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	// Expand the list of seeds with all found in provided directories
	dSeeds, err := readSeedDirs(outFile, inFile, opt.seedDirs, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	seeds = append(seeds, dSeeds...)

	var stats *desync.ExtractStats
	if opt.inPlace {
		stats, err = writeInplace(ctx, outFile, idx, s, seeds, opt.n)
	} else {
		stats, err = writeWithTmpFile(ctx, outFile, idx, s, seeds, opt.n)
	}
	if err != nil {
		return err
	}
	if opt.printStats {
		return printJSON(stdout, stats)
	}
	return nil
}

func writeWithTmpFile(ctx context.Context, name string, idx desync.Index, s desync.Store, seeds []desync.Seed, n int) (*desync.ExtractStats, error) {
	// Prepare a tempfile that'll hold the output during processing. Close it, we
	// just need the name here since it'll be opened multiple times during write.
	// Also make sure it gets removed regardless of any errors below.
	var stats *desync.ExtractStats
	tmp, err := tempfile.NewMode(filepath.Dir(name), "."+filepath.Base(name), 0644)
	if err != nil {
		return stats, err
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	// Build the blob from the chunks, writing everything into the tempfile
	if stats, err = writeInplace(ctx, tmp.Name(), idx, s, seeds, n); err != nil {
		return stats, err
	}

	// Rename the tempfile to the output file
	return stats, os.Rename(tmp.Name(), name)
}

func writeInplace(ctx context.Context, name string, idx desync.Index, s desync.Store, seeds []desync.Seed, n int) (*desync.ExtractStats, error) {
	pb := NewProgressBar("")

	// Build the blob from the chunks, writing everything into given filename
	return desync.AssembleFile(ctx, name, idx, s, seeds, n, pb)
}

func readSeeds(dstFile string, locations []string, opts cmdStoreOptions) ([]desync.Seed, error) {
	var seeds []desync.Seed
	for _, srcIndexFile := range locations {
		srcIndex, err := readCaibxFile(srcIndexFile, opts)
		if err != nil {
			return nil, err
		}
		srcFile := strings.TrimSuffix(srcIndexFile, ".caibx")

		seed, err := desync.NewIndexSeed(dstFile, srcFile, srcIndex)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, seed)
	}
	return seeds, nil
}

func readSeedDirs(dstFile, dstIdxFile string, dirs []string, opts cmdStoreOptions) ([]desync.Seed, error) {
	var seeds []desync.Seed
	absIn, err := filepath.Abs(dstIdxFile)
	if err != nil {
		return nil, err
	}
	for _, dir := range dirs {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if filepath.Ext(path) != ".caibx" {
				return nil
			}
			abs, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			// The index we're trying to extract may be in the same dir, skip it
			if abs == absIn {
				return nil
			}
			// Expect the blob to be there next to the index file, skip the index if not
			srcFile := strings.TrimSuffix(path, ".caibx")
			if _, err := os.Stat(srcFile); err != nil {
				return nil
			}
			// Read the index and add it to the list of seeds
			srcIndex, err := readCaibxFile(path, opts)
			if err != nil {
				return err
			}
			seed, err := desync.NewIndexSeed(dstFile, srcFile, srcIndex)
			if err != nil {
				return err
			}
			seeds = append(seeds, seed)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return seeds, nil
}
