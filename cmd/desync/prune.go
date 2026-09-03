package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type pruneOptions struct {
	cmdStoreOptions
	store string
	yes   bool
}

func newPruneCommand(ctx context.Context) *cobra.Command {
	var opt pruneOptions

	cmd := &cobra.Command{
		Use:   "prune <index> [<index>...]",
		Short: "Remove unreferenced chunks from a store",
		Long: `Read chunk IDs from index files and delete all chunks from a store
that are not referenced in any of the provided index files. This is a
destructive operation; a confirmation prompt is shown before any chunks are
deleted unless --yes is used. Use '-' to read a single index from STDIN.`,
		Example: `  desync prune -s /path/to/local --yes file.caibx
  desync prune -s /path/to/local current.caibx previous.caibx`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPrune(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "store to prune")
	flags.BoolVarP(&opt.yes, "yes", "y", false, "do not ask for confirmation before deleting chunks")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runPrune(ctx context.Context, opt pruneOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	if opt.store == "" {
		return errors.New("no store provided")
	}
	// The confirmation prompt below reads STDIN, which an index read from
	// there has already consumed. Say so rather than ask a question that
	// can't be answered.
	if !opt.yes && slices.Contains(args, "-") {
		return errors.New("--yes is required when reading an index from STDIN")
	}

	// Open the target store
	sr, err := storeFromLocation(opt.store, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	defer sr.Close()

	// Make sure this store can be used for pruning
	s, ok := sr.(desync.PruneStore)
	if !ok {
		if q, ok := sr.(*desync.WriteDedupQueue); ok {
			if s, ok = q.S.(desync.PruneStore); !ok {
				return fmt.Errorf("store '%s' does not support pruning", q.S)
			}
		} else {
			return fmt.Errorf("store '%s' does not support pruning", opt.store)
		}
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	ids := make(map[desync.ChunkID]struct{})
	for _, name := range args {
		c, err := readCaibxFile(name, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		for _, c := range c.Chunks {
			ids[c.ID] = struct{}{}
		}
	}

	// If the -y option wasn't provided, ask the user to confirm before doing anything
	if !opt.yes {
		fmt.Fprintf(stdout, "Warning: The provided index files reference %d unique chunks. Are you sure\nyou want to delete all other chunks from '%s'?\n", len(ids), s)
		in := bufio.NewReader(stdin)
	ask:
		for {
			fmt.Fprint(stdout, "[y/N]: ")
			// Read the whole line rather than a word, so that just hitting
			// enter is the "no" the prompt offers as the default.
			a, err := in.ReadString('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			switch strings.TrimSpace(a) {
			case "y", "Y":
				break ask
			case "n", "N", "":
				return nil
			}
			if errors.Is(err, io.EOF) { // nothing left to answer with
				return nil
			}
		}
	}

	return s.Prune(ctx, ids)
}
