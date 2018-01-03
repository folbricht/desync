package main

import (
	"context"
	"crypto/sha512"
	"flag"
	"fmt"
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

	// Prepare the chunker
	c, err := desync.NewChunker(f, min, avg, max, startPos)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		start, b, err := c.Next()
		if err != nil {
			return err
		}
		if len(b) == 0 {
			return nil
		}
		sum := sha512.Sum512_256(b)
		fmt.Printf("%d\t%d\t%x\n", start, len(b), sum)
	}
}
