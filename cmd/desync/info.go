package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/folbricht/desync"
)

const infoUsage = `desync info [-s <store>] <index>

Displays information about the provided index, such as number of chunks. If a
store is provided, it'll also show how many of the chunks are present in the
store.`

func info(ctx context.Context, args []string) error {
	var (
		n              int
		clientCert     string
		clientKey      string
		storeLocations = new(multiArg)
		showJSON       bool
		results        struct {
			Total   int    `json:"total"`
			Unique  int    `json:"unique"`
			InStore uint64 `json:"in-store"`
			Size    uint64 `json:"size"`
		}
	)
	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, infoUsage)
		flags.PrintDefaults()
	}
	flags.Var(storeLocations, "s", "store location, can be multiples")
	flags.IntVar(&n, "n", 10, "number of goroutines")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.BoolVar(&showJSON, "j", false, "show information in JSON format")
	flags.Parse(args)

	if flags.NArg() < 1 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 1 {
		return errors.New("Too many arguments. See -h for help.")
	}

	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	opts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}

	// Read the index
	c, err := readCaibxFile(flags.Arg(0), opts)
	if err != nil {
		return err
	}

	// Calculate the size of the blob, from the last chunk
	if len(c.Chunks) > 0 {
		last := c.Chunks[len(c.Chunks)-1]
		results.Size = last.Start + last.Size
	}

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

	if len(storeLocations.list) > 0 {
		store, err := multiStore(storeOptions{n: n}, storeLocations.list...)
		if err != nil {
			return err
		}

		// Query the store in parallel for better performance
		var wg sync.WaitGroup
		ids := make(chan desync.ChunkID)
		for i := 0; i < n; i++ {
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

	if showJSON {
		b, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	} else {
		fmt.Println("Blob size:", results.Size)
		fmt.Println("Total chunks:", results.Total)
		fmt.Println("Unique chunks:", results.Unique)
		fmt.Println("Chunks in store:", results.InStore)
	}
	return nil
}
