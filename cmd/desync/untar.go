package main

import (
	"bytes"
	"context"
	"crypto/sha512"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/folbricht/desync"
)

const untarUsage = `desync untar <catar> <target>

Extracts a directory tree from a catar file.`

func untar(ctx context.Context, args []string) error {
	var (
		readIndex     bool
		n             int
		storeLocation string
	)
	flags := flag.NewFlagSet("untar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, untarUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&readIndex, "i", false, "Read index file (caidx), not catar")
	flags.StringVar(&storeLocation, "s", "", "Local casync store location (with -i)")
	flags.IntVar(&n, "n", 10, "number of goroutines (with -i)")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	input := flags.Arg(0)
	targetDir := flags.Arg(1)

	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()

	// If we got a catar file unpack that and exit
	if !readIndex {
		return desync.UnTar(ctx, f, targetDir)
	}

	// Apparently the input must be an index, read it whole
	index, err := desync.IndexFromReader(f)
	if err != nil {
		return err
	}

	// Prepare the store
	s, err := desync.NewLocalStore(storeLocation)
	if err != nil {
		return err
	}

	return untarIndex(ctx, targetDir, index, s, n)
}

func untarIndex(ctx context.Context, dst string, index desync.Index, s desync.Store, n int) error {
	type requestJob struct {
		chunk desync.IndexChunk // requested chunk
		data  chan ([]byte)     // channel for the (decompressed) chunk
	}
	var (
		// stop bool
		wg       sync.WaitGroup
		mu       sync.Mutex
		pErr     error
		req      = make(chan requestJob)
		assemble = make(chan chan []byte, n)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if pErr == nil {
			pErr = err
		}
		cancel()
	}

	// Use a pipe as input to untar and write the chunks into that (in the right
	// order of course)
	r, w := io.Pipe()

	// Workers - getting chunks from the store
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for r := range req {
				// Pull the (compressed) chunk from the store
				b, err := s.GetChunk(r.chunk.ID)
				if err != nil {
					recordError(err)
					continue
				}
				// Since we know how big the chunk is supposed to be, pre-allocate a
				// slice to decompress into
				db := make([]byte, r.chunk.Size)
				// The the chunk is compressed. Decompress it here
				db, err = desync.Decompress(db, b)
				if err != nil {
					recordError(err)
					continue
				}
				// Verify the checksum of the chunk matches the ID
				sum := sha512.Sum512_256(db)
				if sum != r.chunk.ID {
					recordError(fmt.Errorf("unexpected sha512/256 %s for chunk id %s", sum, r.chunk.ID))
					continue
				}
				// Might as well verify the chunk size while we're at it
				if r.chunk.Size != uint64(len(db)) {
					recordError(fmt.Errorf("unexpected size for chunk %s", r.chunk.ID))
					continue
				}
				r.data <- db
				close(r.data)
			}
			wg.Done()
		}()
	}

	// Feeder - requesting chunks from the workers
	go func() {
	loop:
		for _, c := range index.Chunks {
			// See if we're meant to stop
			select {
			case <-ctx.Done():
				break loop
			default:
			}
			data := make(chan []byte, 1)
			req <- requestJob{chunk: c, data: data} // request the chunk
			assemble <- data                        // and hand over the data channel to the assembler
		}
		close(req)
		wg.Wait()       // wait for the workers to stop
		close(assemble) // tell the assembler we're done
	}()

	// Assember - push the chunks into the pipe that untar reads from
	go func() {
		for data := range assemble {
			b := <-data
			if _, err := io.Copy(w, bytes.NewReader(b)); err != nil {
				recordError(err)
			}
		}
		w.Close() // No more chunks to come, stop the untar
	}()

	// Run untar in the main go routine
	if err := desync.UnTar(ctx, r, dst); err != nil {
		return err
	}
	return pErr
}
