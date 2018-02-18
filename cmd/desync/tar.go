// +build !windows

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/folbricht/desync"
)

const tarUsage = `desync tar <catar> <source>

Encodes a directory tree into a catar archive or alternatively an index file
with the archive chunked in a local store.`

func tar(ctx context.Context, args []string) error {
	var (
		makeIndex     bool
		n             int
		storeLocation string
		chunkSize     string
	)
	flags := flag.NewFlagSet("tar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, tarUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&makeIndex, "i", false, "Create index file (caidx), not catar")
	flags.StringVar(&storeLocation, "s", "", "Local casync store location (with -i)")
	flags.IntVar(&n, "n", 10, "number of goroutines (with -i)")
	flags.StringVar(&chunkSize, "m", "16:64:256", "Min/Avg/Max chunk size in kb (with -i)")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	output := flags.Arg(0)
	sourceDir := flags.Arg(1)

	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()

	// Just make the catar and stop if that's all that was required
	if !makeIndex {
		return desync.Tar(ctx, f, sourceDir)
	}

	// An index is requested, so stream the output of the tar command directly
	// into a chunker using a pipe
	r, w := io.Pipe()

	// Open the target store
	s, err := WritableStore(n, storeLocation)
	if err != nil {
		return err
	}
	defer s.Close()

	// Prepare the chunker
	min, avg, max, err := parseChunkSizeParam(chunkSize)
	if err != nil {
		return err
	}
	c, err := desync.NewChunker(r, min, avg, max)
	if err != nil {
		return err
	}

	// Run the tar bit in a goroutine, writing to the pipe
	var tarErr error
	go func() {
		tarErr = desync.Tar(ctx, w, sourceDir)
		w.Close()
	}()

	// Read from the pipe, split the stream and store the chunks. This should
	// complete when Tar is done and closes the pipe writer
	index, err := desync.ChunkStream(ctx, c, s, n)
	if err != nil {
		return err
	}

	// See if Tar encountered an error along the way
	if tarErr != nil {
		return tarErr
	}

	// Write the index to file
	i, err := os.Create(output)
	if err != nil {
		return err
	}
	defer i.Close()
	_, err = index.WriteTo(i)
	return err
}
