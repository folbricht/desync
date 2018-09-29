package main

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type chunkOptions struct {
	chunkSize string
	startPos  uint64
}

func newChunkCommand(ctx context.Context) *cobra.Command {
	var opt chunkOptions

	cmd := &cobra.Command{
		Use:     "chunk <file>",
		Short:   "Chunk input file and print chunk points plus chunk ID",
		Long:    `Write start/length/hash pairs for each chunk a file would be split into.`,
		Example: `  desync chunk file.bin`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChunk(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.Uint64VarP(&opt.startPos, "start", "S", 0, "starting position")
	flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in kb")
	return cmd
}

func runChunk(ctx context.Context, opt chunkOptions, args []string) error {
	min, avg, max, err := parseChunkSizeParam(opt.chunkSize)
	if err != nil {
		return err
	}

	dataFile := args[0]

	// Open the blob
	f, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()
	s, err := f.Seek(int64(opt.startPos), io.SeekStart)
	if err != nil {
		return err
	}
	if uint64(s) != opt.startPos {
		return fmt.Errorf("requested seek to position %d, but got %d", opt.startPos, s)
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
		start, b, err := c.Next()
		if err != nil {
			return err
		}
		if len(b) == 0 {
			return nil
		}
		sum := sha512.Sum512_256(b)
		fmt.Printf("%d\t%d\t%x\n", start+opt.startPos, len(b), sum)
	}
}
