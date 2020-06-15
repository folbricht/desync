package main

import (
	"bufio"
	"context"
	"errors"
	"os"
	"strings"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type chopOptions struct {
	cmdStoreOptions
	store         string
	ignoreIndexes []string
	ignoreChunks  []string
}

func newChopCommand(ctx context.Context) *cobra.Command {
	var opt chopOptions

	cmd := &cobra.Command{
		Use:   "chop <index> <file>",
		Short: "Reads chunks from a file according to an index",
		Long: `Reads the index and extracts all referenced chunks from the file into a store,
local or remote.

Does not modify the input file or index in any. It's used to populate a chunk
store by chopping up a file according to an existing index. To exclude chunks that
are known to exist in the target store already, use --ignore <index> which will
skip any chunks from the given index. The same can be achieved by providing the
chunks in their ASCII representation in a text file with --ignore-chunks <file>.

Use '-' to read the index from STDIN.`,
		Example: `  desync chop -s sftp://192.168.1.1/store file.caibx largefile.bin`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChop(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store")
	flags.StringSliceVarP(&opt.ignoreIndexes, "ignore", "", nil, "index(s) to ignore chunks from")
	flags.StringSliceVarP(&opt.ignoreChunks, "ignore-chunks", "", nil, "ignore chunks from text file")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runChop(ctx context.Context, opt chopOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if opt.store == "" {
		return errors.New("no target store provided")
	}

	indexFile := args[0]
	dataFile := args[1]

	// Open the target store
	s, err := WritableStore(opt.store, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	defer s.Close()

	// Read the input
	c, err := readCaibxFile(indexFile, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	chunks := c.Chunks

	// If requested, skip/ignore all chunks that are referenced in other indexes or text files
	if len(opt.ignoreIndexes) > 0 || len(opt.ignoreChunks) > 0 {
		m := make(map[desync.ChunkID]desync.IndexChunk)
		for _, c := range chunks {
			m[c.ID] = c
		}

		// Remove chunks referenced in indexes
		for _, f := range opt.ignoreIndexes {
			i, err := readCaibxFile(f, opt.cmdStoreOptions)
			if err != nil {
				return err
			}
			for _, c := range i.Chunks {
				delete(m, c.ID)
			}
		}

		// Remove chunks referenced in ASCII text files
		for _, f := range opt.ignoreChunks {
			ids, err := readChunkIDFile(f)
			if err != nil {
				return err
			}
			for _, id := range ids {
				delete(m, id)
			}
		}

		chunks = make([]desync.IndexChunk, 0, len(m))
		for _, c := range m {
			chunks = append(chunks, c)
		}
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar("")

	// Chop up the file into chunks and store them in the target store
	return desync.ChopFile(ctx, dataFile, chunks, s, opt.n, pb)
}

// Read a list of chunk IDs from a file. Blank lines are skipped.
func readChunkIDFile(file string) ([]desync.ChunkID, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ids []desync.ChunkID
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		id, err := desync.ChunkIDFromString(line)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, scanner.Err()
}
