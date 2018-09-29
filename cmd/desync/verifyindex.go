package main

import (
	"context"
	"errors"

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
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	return cmd
}
func runVerifyIndex(ctx context.Context, opt verifyIndexOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	indexFile := args[0]
	dataFile := args[1]

	// Read the input
	idx, err := readCaibxFile(indexFile, opt.cmdStoreOptions)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar("")

	// Chop up the file into chunks and store them in the target store
	return desync.VerifyIndex(ctx, dataFile, idx, opt.n, pb)
}
