package main

import (
	"context"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type verifyIndexOptions struct {
	cmdStoreOptions
}

func newVerifyIndexCommand(ctx context.Context) *cobra.Command {
	var opt verifyIndexOptions

	cmd := &cobra.Command{
		Use:   "verify-index <index> <file>",
		Short: "Verifies an index matches a file",
		Long: `Verifies an index file matches the content of a blob. Use '-' to read the index
from STDIN.`,
		Example: `  desync verify-index sftp://192.168.1.1/myIndex.caibx largefile.bin`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVerifyIndex(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}
func runVerifyIndex(ctx context.Context, opt verifyIndexOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	indexFile := args[0]
	dataFile := args[1]

	// Read the input
	idx, err := readCaibxFile(indexFile, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := desync.NewProgressBar("")

	// Chop up the file into chunks and store them in the target store
	return desync.VerifyIndex(ctx, dataFile, idx, opt.n, pb)
}
