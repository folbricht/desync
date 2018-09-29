// +build !windows

package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type tarOptions struct {
	cmdStoreOptions
	store       string
	chunkSize   string
	createIndex bool
}

func newTarCommand(ctx context.Context) *cobra.Command {
	var opt tarOptions

	cmd := &cobra.Command{
		Use:   "tar <catar|index> <source>",
		Short: "Store a directory tree in a catar archive or index",
		Long: `Encodes a directory tree into a catar archive or alternatively an index file
with the archive chunked into a store. Use '-' to write the output,
catar or index to STDOUT.`,
		Example: `  desync tar documents.catar $HOME/Documents
  desync make -s /path/to/local pics.caibx $HOME/Pictures`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTar(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store (used with -i)")
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in kb")
	flags.BoolVarP(&opt.createIndex, "index", "i", false, "create index file (caidx), not catar")
	return cmd
}

func runTar(ctx context.Context, opt tarOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	if opt.createIndex && opt.store == "" {
		return errors.New("-i requires a store (-s <location>)")
	}

	output := args[0]
	sourceDir := args[1]

	// Just make the catar and stop if that's all that was required
	if !opt.createIndex {
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

	// An index is requested, so stream the output of the tar command directly
	// into a chunker using a pipe
	r, w := io.Pipe()

	// Open the target store
	s, err := WritableStore(opt.store, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	defer s.Close()

	// Prepare the chunker
	min, avg, max, err := parseChunkSizeParam(opt.chunkSize)
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
	index, err := desync.ChunkStream(ctx, c, s, opt.n)
	if err != nil {
		return err
	}

	index.Index.FeatureFlags |= desync.TarFeatureFlags

	// See if Tar encountered an error along the way
	if tarErr != nil {
		return tarErr
	}

	// Write the index
	return storeCaibxFile(index, output, opt.cmdStoreOptions)
}
