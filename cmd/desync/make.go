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

Creates chunks from the input file and builds an index. If a chunk store is
provided with -s, such as a local directory or S3 store, it split the input
file according to the index and stores the chunks.`

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
	flags.StringVar(&storeLocation, "s", "", "Chunk store location")
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

	// Open the target store if one was given
	var s desync.WriteStore
	if storeLocation != "" {
		s, err = WritableStore(storeLocation, storeOptions{n: n})
		if err != nil {
			return err
		}
		defer s.Close()
	}

	// Split up the file and create and index from it
	pb := NewProgressBar("Chunking ")
	index, stats, err := desync.IndexFromFile(ctx, dataFile, n, min, avg, max, pb)
	if err != nil {
		return err
	}

	// Chop up the file into chunks and store them in the target store if a store was given
	if s != nil {
		pb := NewProgressBar("Storing ")
		if err := desync.ChopFile(ctx, dataFile, index.Chunks, s, n, pb); err != nil {
			return err
		}
	}

	fmt.Println("Chunks produced:", stats.ChunksAccepted)
	fmt.Println("Overhead:", stats.ChunksProduced-stats.ChunksAccepted)

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
