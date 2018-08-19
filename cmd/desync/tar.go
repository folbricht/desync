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

const tarUsage = `desync tar <catar|index> <source>

Encodes a directory tree into a catar archive or alternatively an index file
with the archive chunked in a local or S3 store. Use '-' to write the output,
catar or index to STDOUT.`

func tar(ctx context.Context, args []string) error {
	var (
		makeIndex     bool
		n             int
		clientCert    string
		clientKey     string
		storeLocation string
		chunkSize     string
	)
	flags := flag.NewFlagSet("tar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, tarUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&makeIndex, "i", false, "Create index file (caidx), not catar")
	flags.StringVar(&storeLocation, "s", "", "Local or S3 casync store location (with -i)")
	flags.IntVar(&n, "n", 10, "number of goroutines (with -i)")
	flags.StringVar(&clientCert, "clientCert", "", "Path to Client Certificate for TLS authentication")
	flags.StringVar(&clientKey, "clientKey", "", "Path to Client Key for TLS authentication")
	flags.StringVar(&chunkSize, "m", "16:64:256", "Min/Avg/Max chunk size in kb (with -i)")
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}
	if makeIndex && storeLocation == "" {
		return errors.New("-i requires a store (-s <location>)")
	}
	if clientKey != "" && clientCert == "" || clientCert != "" && clientKey == "" {
		return errors.New("-clientKey and -clientCert options need to be provided together.")
	}

	output := flags.Arg(0)
	sourceDir := flags.Arg(1)

	// Just make the catar and stop if that's all that was required
	if !makeIndex {
		var w io.Writer
		if output == "-" {
			w = os.Stdout
		} else {
			f, err := os.Create(output)
			if err != nil {
				return err
			}
			defer f.Close()
			w = f
		}
		return desync.Tar(ctx, w, sourceDir)
	}

	sOpts := storeOptions{
		n:          n,
		clientCert: clientCert,
		clientKey:  clientKey,
	}

	// An index is requested, so stream the output of the tar command directly
	// into a chunker using a pipe
	r, w := io.Pipe()

	// Open the target store
	s, err := WritableStore(storeLocation, sOpts)
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

	// Write the index
	return storeCaibxFile(index, output, sOpts)
}
