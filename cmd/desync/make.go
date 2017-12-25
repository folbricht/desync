package main

import (
	"context"
	"crypto/sha512"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

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
		fmt.Fprintln(os.Stderr, chopUsage)
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

	min, avg, max, err := splitChunkSizes(chunkSize)
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

	// Split the files into chunks, store the chunks, and return index chunks
	// for use in an index
	chunks, errs := makeIndexChunks(ctx, c, s, n)
	if len(errs) != 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}

	// Build the index
	index := desync.Index{
		Index: desync.FormatIndex{
			FeatureFlags: desync.CaFormatExcludeNoDump | desync.CaFormatSHA512256,
			ChunkSizeMin: uint64(min),
			ChunkSizeAvg: uint64(avg),
			ChunkSizeMax: uint64(max),
		},
		Chunks: chunks,
	}

	// Write it to the index file
	i, err := os.Create(indexFile)
	if err != nil {
		return err
	}
	defer i.Close()
	_, err = index.WriteTo(i)
	return err
}

func makeIndexChunks(ctx context.Context, c desync.Chunker, s desync.LocalStore, n int) ([]desync.IndexChunk, []error) {
	type chunkJob struct {
		id desync.ChunkID
		b  []byte
	}
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		errs   []error
		in     = make(chan chunkJob)
		chunks []desync.IndexChunk
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
		cancel()
	}

	// Start the workers responsible for compression and storage of the chunks
	// produced by the feeder
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for c := range in {
				// Skip this chunk if the store already has it
				if s.HasChunk(c.id) {
					continue
				}

				// Compress the chunk
				cb, err := desync.Compress(c.b)
				if err != nil {
					recordError(err)
					continue
				}

				// And store it
				if err = s.StoreChunk(c.id, cb); err != nil {
					recordError(err)
					continue
				}
			}
			wg.Done()
		}()
	}

	// Feed the workers, stop if there are any errors. To keep the index list in
	// order, we calculate the checksum here before handing	them over to the
	// workers for compression and storage. That could probablybe optimized further
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break
		default:
		}
		start, b, err := c.Next()
		if err != nil {
			return chunks, []error{err}
		}
		if len(b) == 0 {
			break
		}
		// Calculate this chunks checksum and build an index record
		sum := sha512.Sum512_256(b)
		chunk := desync.IndexChunk{Start: start, Size: uint64(len(b)), ID: sum}
		chunks = append(chunks, chunk)

		// Send it off for compression and storage
		in <- chunkJob{id: sum, b: b}
	}
	close(in)
	wg.Wait()

	return chunks, errs
}

func splitChunkSizes(s string) (min, avg, max uint64, err error) {
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
