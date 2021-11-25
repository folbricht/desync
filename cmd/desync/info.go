package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type infoOptions struct {
	cmdStoreOptions
	stores      []string
	seeds       []string
	printFormat string
}

func newInfoCommand(ctx context.Context) *cobra.Command {
	var opt infoOptions

	cmd := &cobra.Command{
		Use:   "info <index>",
		Short: "Show information about an index",
		Long: `Displays information about the provided index, such as the number of chunks
and the total size of unique chunks that are not available in the seed. If a
store is provided, it'll also show how many of the chunks are present in the
store. If one or more seed indexes are provided, the number of chunks available
in the seeds are also shown. Use '-' to read the index from STDIN.`,
		Example: `  desync info -s /path/to/local --format=json file.caibx`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInfo(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s)")
	flags.StringSliceVar(&opt.seeds, "seed", nil, "seed indexes")
	flags.StringVarP(&opt.printFormat, "format", "f", "json", "output format, plain or json")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runInfo(ctx context.Context, opt infoOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}

	// Read the index
	c, err := readCaibxFile(args[0], opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	var results struct {
		Total         int    `json:"total"`
		Unique        int    `json:"unique"`
		InStore       uint64 `json:"in-store"`
		InSeed        uint64 `json:"in-seed"`
		Size          uint64 `json:"size"`
		SizeNotInSeed uint64 `json:"dedup-size-not-in-seed"`
		ChunkSizeMin  uint64 `json:"chunk-size-min"`
		ChunkSizeAvg  uint64 `json:"chunk-size-avg"`
		ChunkSizeMax  uint64 `json:"chunk-size-max"`
	}

	dedupedSeeds := make(map[desync.ChunkID]struct{})
	for _, seed := range opt.seeds {
		caibxSeed, err := readCaibxFile(seed, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		for _, chunk := range caibxSeed.Chunks {
			dedupedSeeds[chunk.ID] = struct{}{}
			select {
			case <-ctx.Done():
				return nil
			default:
			}
		}
	}

	// Calculate the size of the blob, from the last chunk
	if len(c.Chunks) > 0 {
		last := c.Chunks[len(c.Chunks)-1]
		results.Size = last.Start + last.Size
	}

	// Capture min:avg:max from the index
	results.ChunkSizeMin = c.Index.ChunkSizeMin
	results.ChunkSizeAvg = c.Index.ChunkSizeAvg
	results.ChunkSizeMax = c.Index.ChunkSizeMax

	// Go through each chunk from the index to count them, de-dup each chunks
	// with a map and calculate the size of the chunks that are not available
	// in seed
	deduped := make(map[desync.ChunkID]struct{})
	for _, chunk := range c.Chunks {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		results.Total++
		if _, duplicatedChunk := deduped[chunk.ID]; duplicatedChunk {
			// This is a duplicated chunk, do not count it again in the seed
			continue
		}

		deduped[chunk.ID] = struct{}{}
		if _, isAvailable := dedupedSeeds[chunk.ID]; isAvailable {
			// This chunk is available in the seed
			results.InSeed++
		} else {
			// The seed doesn't have this chunk, sum its size
			results.SizeNotInSeed += chunk.Size
		}
	}
	results.Unique = len(deduped)

	if len(opt.stores) > 0 {
		store, err := multiStoreWithRouter(cmdStoreOptions{n: opt.n}, opt.stores...)
		if err != nil {
			return err
		}

		// Query the store in parallel for better performance
		var wg sync.WaitGroup
		ids := make(chan desync.ChunkID)
		for i := 0; i < opt.n; i++ {
			wg.Add(1)
			go func() {
				for id := range ids {
					if hasChunk, err := store.HasChunk(id); err == nil && hasChunk {
						atomic.AddUint64(&results.InStore, 1)
					}
				}
				wg.Done()
			}()
		}
		for id := range deduped {
			ids <- id
		}
		close(ids)
		wg.Wait()
	}

	switch opt.printFormat {
	case "json":
		if err := printJSON(stdout, results); err != nil {
			return err
		}
	case "plain":
		fmt.Println("Blob size:", results.Size)
		fmt.Println("Size of deduplicated chunks not in seed:", results.SizeNotInSeed)
		fmt.Println("Total chunks:", results.Total)
		fmt.Println("Unique chunks:", results.Unique)
		fmt.Println("Chunks in store:", results.InStore)
		fmt.Println("Chunks in seed:", results.InSeed)
		fmt.Println("Chunk size min:", results.ChunkSizeMin)
		fmt.Println("Chunk size avg:", results.ChunkSizeAvg)
		fmt.Println("Chunk size max:", results.ChunkSizeMax)
	default:
		return fmt.Errorf("unsupported output format '%s", opt.printFormat)
	}
	return nil
}
