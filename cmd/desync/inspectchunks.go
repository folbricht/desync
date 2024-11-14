package main

import (
	"context"
	"fmt"
	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
	"io"
	"os"
)

type inspectChunksOptions struct {
	cmdStoreOptions
	store string
}

func newinspectChunksCommand(ctx context.Context) *cobra.Command {
	var opt inspectChunksOptions

	cmd := &cobra.Command{
		Use:   "inspect-chunks <index> [<output>]",
		Short: "Inspect chunks from an index and an optional local store",
		Long: `Prints a detailed JSON with information about chunks stored in an index file.
By using the '--store' option to provide a local store, the generated JSON will include, if
available, the chunks compressed size info from that particular store.`,
		Example: `  desync inspect-chunks file.caibx
desync inspect-chunks --store /mnt/store file.caibx inspect_result.json`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInspectChunks(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "local source store")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runInspectChunks(ctx context.Context, opt inspectChunksOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}

	var (
		outFile io.Writer
		err     error
	)
	if len(args) == 2 {
		outFileName := args[1]
		outFile, err = os.Create(outFileName)
		if err != nil {
			return err
		}
	} else {
		outFile = stdout
	}

	// Read the input
	c, err := readCaibxFile(args[0], opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	var (
		chunksInfo []desync.ChunkAdditionalInfo
		s          desync.LocalStore
	)

	if opt.store != "" {
		sr, err := storeFromLocation(opt.store, opt.cmdStoreOptions)
		if err != nil {
			return err
		}

		// We expect a local store, it is an error to provide something different
		var ok bool
		s, ok = sr.(desync.LocalStore)

		if !ok {
			return fmt.Errorf("'%s' is not a local store", opt.store)
		}
	}

	for _, chunk := range c.Chunks {
		var size int64 = 0
		// Get the compressed size only if the store actually has compressed chunks
		if opt.store != "" && !s.Opt.Uncompressed {
			size, _ = s.GetChunkSize(chunk.ID)
		}

		chunksInfo = append(chunksInfo, desync.ChunkAdditionalInfo{
			ID:               chunk.ID,
			UncompressedSize: chunk.Size,
			CompressedSize:   size,
		})
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}

	return printJSON(outFile, chunksInfo)
}
