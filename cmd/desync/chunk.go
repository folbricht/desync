package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
)

const chunkUsage = `desync chunk [options] <file>

Write start/length/hash pairs for each chunk a file is split into.`

func chunkCmd(ctx context.Context, args []string) error {
	var (
		chunkSize string
		startPos  uint64
	)
	flags := flag.NewFlagSet("chunk", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, chunkUsage)
		flags.PrintDefaults()
	}
	flags.StringVar(&chunkSize, "m", "16:64:256", "Min/Avg/Max chunk size in kb")
	flags.Uint64Var(&startPos, "S", 0, "Starting position")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 1 {
		return errors.New("Too many arguments. See -h for help.")
	}

	min, avg, max, err := parseChunkSizeParam(chunkSize)
	if err != nil {
		return err
	}

	dataFile := flags.Arg(0)

	// Open the blob
	f, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()
	s, err := f.Seek(int64(startPos), io.SeekStart)
	if err != nil {
		return err
	}
	if uint64(s) != startPos {
		return fmt.Errorf("requested seek to position %d, but got %d", startPos, s)
	}

	// Prepare the chunker
	c, err := desync.NewChunker(f, min, avg, max)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		chunk, err := c.Next()
		if err != nil {
			return err
		}
		if len(chunk.Data) == 0 {
			return nil
		}
		fmt.Printf("%d\t%d\t%x\n", chunk.Start+startPos, len(chunk.Data), chunk.ID)
	}
}
