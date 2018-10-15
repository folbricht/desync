package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type infoOptions struct {
	cmdStoreOptions
	stores      []string
	printFormat string
}

func newInfoCommand(ctx context.Context) *cobra.Command {
	var opt infoOptions

	cmd := &cobra.Command{
		Use:   "info <index>",
		Short: "Show information about an index",
		Long: `Displays information about the provided index, such as number of chunks. If a
store is provided, it'll also show how many of the chunks are present in the
store. Use '-' to read the index from STDIN.`,
		Example: `  desync info -s /path/to/local --format=json file.caibx`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInfo(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVarP(&opt.stores, "store", "s", nil, "source store(s)")
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")

	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	flags.StringVarP(&opt.printFormat, "format", "f", "json", "output format, plain or json")
	return cmd
}

func runInfo(ctx context.Context, opt infoOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}

	// Read the index
	c, err := readCaibxFile(args[0], opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	var results struct {
		Total        int    `json:"total"`
		Unique       int    `json:"unique"`
		InStore      uint64 `json:"in-store"`
		Size         uint64 `json:"size"`
		ChunkSizeMin uint64 `json:"chunk-size-min"`
		ChunkSizeAvg uint64 `json:"chunk-size-avg"`
		ChunkSizeMax uint64 `json:"chunk-size-max"`
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

	// Go through each chunk to count and de-dup them with a map
	deduped := make(map[desync.ChunkID]struct{})
	for _, chunk := range c.Chunks {
		results.Total++
		deduped[chunk.ID] = struct{}{}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	results.Unique = len(deduped)

	if len(opt.stores) > 0 {
		store, err := multiStore(cmdStoreOptions{n: opt.n}, opt.stores...)
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
					if store.HasChunk(id) {
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
		fmt.Println("Total chunks:", results.Total)
		fmt.Println("Unique chunks:", results.Unique)
		fmt.Println("Chunks in store:", results.InStore)
		fmt.Println("Chunk size min:", results.ChunkSizeMin)
		fmt.Println("Chunk size avg:", results.ChunkSizeAvg)
		fmt.Println("Chunk size max:", results.ChunkSizeMax)
	default:
		return fmt.Errorf("unsupported output format '%s", opt.printFormat)
	}
	return nil
}
