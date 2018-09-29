package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/folbricht/desync"
	"github.com/folbricht/tempfile"
)

const extractUsage = `desync extract [options] <index> <output>

Reads an index and builds a blob reading chunks from one or more chunk stores.
When using -k, the blob will be extracted in-place utilizing existing data and
the target file will not be deleted on error. This can be used to restart a
failed prior extraction without having to retrieve completed chunks again.
Muptiple optional seed indexes can be given with -seed. The matching blob needs
to have the same name as the indexfile without the .caibx extension. If several
seed files and indexes are available, the -seed-dir option can be used to
automatically select call .caibx files in a directory as seeds. Use '-' to read
the index from STDIN.`

func extract(ctx context.Context, args []string) error {
	var (
		cacheLocation    string
		n                int
		err              error
		storeLocations   = new(multiArg)
		seedLocations    = new(multiArg)
		seedDirLocations = new(multiArg)
		clientCert       string
		clientKey        string
		inPlace          bool
		printStats       bool
	)
	flags := flag.NewFlagSet("extract", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, extractUsage)
		flags.PrintDefaults()
	}

	flags.Var(storeLocations, "s", "casync store location, can be multiples")
	flags.Var(seedLocations, "seed", "seed indexes, can be multiples")
	flags.Var(seedDirLocations, "seed-dir", "directory with seed index files, can be multiples")
	flags.StringVar(&cacheLocation, "c", "", "use local store as cache")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.BoolVar(&desync.TrustInsecure, "t", false, "trust invalid certificates")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.BoolVar(&inPlace, "k", false, "extract the file in place and keep it in case of error")
	flags.BoolVar(&printStats, "stats", false, "Print statistics in JSON format")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	inFile := flags.Arg(0)
	outFile := flags.Arg(1)
	if inFile == outFile {
		return errors.New("Input and output filenames match.")
	}

	// Checkout the store
	if len(storeLocations.list) == 0 {
		return errors.New("No casync store provided. See -h for help.")
	}

	// Parse the store locations, open the stores and add a cache is requested
	var s desync.Store
	opts := cmdStoreOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}
	s, err = MultiStoreWithCache(opts, cacheLocation, storeLocations.list...)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	idx, err := readCaibxFile(inFile, opts)
	if err != nil {
		return err
	}

	// Build a list of seeds if any were given in the command line
	seeds, err := readSeeds(outFile, seedLocations.list, opts)
	if err != nil {
		return err
	}

	// Expand the list of seeds with all found in provided directories
	dSeeds, err := readSeedDirs(outFile, inFile, seedDirLocations.list, opts)
	if err != nil {
		return err
	}
	seeds = append(seeds, dSeeds...)

	var stats *desync.ExtractStats
	if inPlace {
		stats, err = writeInplace(ctx, outFile, idx, s, seeds, n)
	} else {
		stats, err = writeWithTmpFile(ctx, outFile, idx, s, seeds, n)
	}
	if err != nil {
		return err
	}
	if printStats {
		return printJSON(stats)
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
