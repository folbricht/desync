package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/pkg/chunkers"
	"github.com/spf13/cobra"
)

type chunkOptions struct {
	cmdChunkerOptions
	startPos uint64
}

func newChunkCommand(ctx context.Context) *cobra.Command {
	var opt chunkOptions

	cmd := &cobra.Command{
		Use:   "chunk <file>",
		Short: "Chunk input file and print chunk boundaries and IDs",
		Long: `Chunks the input file without storing anything, and prints a start/length/hash
triple for each chunk the file would be split into. Useful to inspect or tune
chunking parameters before running 'make'.`,
		Example: `  desync chunk file.bin`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChunk(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.Uint64VarP(&opt.startPos, "start", "S", 0, "starting position in bytes")
	addChunkerOptions(&opt.cmdChunkerOptions, flags)
	return cmd
}

func runChunk(ctx context.Context, opt chunkOptions, args []string) error {
	chunkerParams, err := opt.cmdChunkerOptions.ToChunkerParams()
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
	c, err := chunkers.NewChunker(opt.cmdChunkerOptions.name, f, chunkerParams)
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
		sum := desync.Digest.Sum(b)
		fmt.Fprintf(stdout, "%d\t%d\t%x\n", start+opt.startPos, len(b), sum)
	}
}
