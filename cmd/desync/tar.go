package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type tarOptions struct {
	cmdStoreOptions
	store       string
	chunkSize   string
	createIndex bool
	desync.LocalFSOptions
	inFormat string
	desync.TarReaderOptions
}

func newTarCommand(ctx context.Context) *cobra.Command {
	var opt tarOptions

	cmd := &cobra.Command{
		Use:   "tar <catar|index> <source>",
		Short: "Store a directory tree in a catar archive or index",
		Long: `Encodes a directory tree into a catar archive or alternatively an index file
with the archive chunked into a store. Use '-' to write the output,
catar or index to STDOUT.

If the desired output is an index file (caidx) rather than a catar,
the -i option can be provided as well as a store. Using -i is equivalent
to first using the tar command to create a catar, then the make
command to chunk it into a store and produce an index file. With -i,
less disk space is required as no intermediary catar is created. There
can however be a difference in performance depending on file size.

By default, input is read from local disk. Using --input-format=tar,
the input can be a tar file or stream to STDIN with '-'.
`,
		Example: `  desync tar documents.catar $HOME/Documents
  desync tar -i -s /path/to/local pics.caidx $HOME/Pictures`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTar(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store (used with -i)")
	flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in kb")
	flags.BoolVarP(&opt.createIndex, "index", "i", false, "create index file (caidx), not catar")
	flags.StringVar(&opt.inFormat, "input-format", "disk", "input format, 'disk' or 'tar'")
	flags.BoolVarP(&opt.NoTime, "no-time", "", false, "set file timestamps to zero in the archive")
	flags.BoolVarP(&opt.AddRoot, "tar-add-root", "", false, "pretend that all tar elements have a common root directory")

	if runtime.GOOS != "windows" {
		flags.BoolVarP(&opt.OneFileSystem, "one-file-system", "x", false, "don't cross filesystem boundaries")
	}

	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runTar(ctx context.Context, opt tarOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if opt.createIndex && opt.store == "" {
		return errors.New("-i requires a store (-s <location>)")
	}
	if opt.AddRoot && opt.inFormat != "tar" {
		return errors.New("--tar-add-root works only with --input-format tar")
	}

	output := args[0]
	source := args[1]

	// Prepare input
	var (
		fs  desync.FilesystemReader
		err error
	)
	switch opt.inFormat {
	case "disk": // Local filesystem
		local := desync.NewLocalFS(source, opt.LocalFSOptions)
		fs = local
	case "tar": // tar archive (different formats), either file or STDOUT
		var r *os.File
		if source == "-" {
			r = os.Stdin
		} else {
			r, err = os.Open(source)
			if err != nil {
				return err
			}
			defer r.Close()
		}
		fs = desync.NewTarReader(r, opt.TarReaderOptions)
	default:
		return fmt.Errorf("invalid input format '%s'", opt.inFormat)
	}

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
		return desync.Tar(ctx, w, fs)
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
		tarErr = desync.Tar(ctx, w, fs)
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
