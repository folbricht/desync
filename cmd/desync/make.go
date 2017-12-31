package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
)

const makeUsage = `desync make [options] <index> <file>

Creates chunks from the input file, stores them in a local store and writes
an index file.`

func makeCmd(ctx context.Context, args []string) error {
	var (
		storeLocation string
		n             int
		chunkSize     string
	)
	flags := flag.NewFlagSet("make", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, makeUsage)
		flags.PrintDefaults()
	}
	flags.StringVar(&storeLocation, "s", "", "Local casync store location")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.StringVar(&chunkSize, "m", "16:64:256", "Min/Avg/Max chunk size in kb")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	min, avg, max, err := parseChunkSizeParam(chunkSize)
	if err != nil {
		return err
	}

	indexFile := flags.Arg(0)
	dataFile := flags.Arg(1)

	// Open the target store
	s, err := desync.NewLocalStore(storeLocation)
	if err != nil {
		return err
	}

	// Open the blob
	f, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Prepare the chunker
	c, err := desync.NewChunker(f, min, avg, max)
	if err != nil {
		return err
	}

	// Split the files into chunks, store the chunks, and return an index
	index, errs := desync.IndexFromBlob(ctx, c, s, n)
	if len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}

	// Write the index to file
	i, err := os.Create(indexFile)
	if err != nil {
		return err
	}
	defer i.Close()
	_, err = index.WriteTo(i)
	return err
}

func parseChunkSizeParam(s string) (min, avg, max uint64, err error) {
	sizes := strings.Split(s, ":")
	if len(sizes) != 3 {
		return 0, 0, 0, fmt.Errorf("Invalid chunk size '%s'. See -h for help.", s)
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
