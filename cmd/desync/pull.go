package main

import (
	"context"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type pullOptions struct{}

func newPullCommand(ctx context.Context) *cobra.Command {
	var opt pullOptions

	cmd := &cobra.Command{
		Use:   "pull - - - <store>",
		Short: "Serve chunks via casync protocol over SSH",
		Long: `Serves up chunks (read-only) from a local store using the casync protocol
via Stdin/Stdout. Functions as a drop-in replacement for casync on remote
stores accessed with SSH. See CASYNC_REMOTE_PATH environment variable.`,
		Example: `  desync pull - - - /path/to/store`,
		Args:    cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPull(ctx, opt, args)
		},
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
	}
	return cmd
}

func runPull(ctx context.Context, opt pullOptions, args []string) error {
	storeLocation := args[3]

	// SSH only supports serving compressed chunks currently. And we really
	// don't want to have to decompress every chunk to verify its checksum.
	// Clients will do that anyway, so disable verification here.
	sOpt, err := cfg.GetStoreOptionsFor(storeLocation)
	if err != nil {
		return err
	}
	sOpt.SkipVerify = true

	// Open the local store to serve chunks from
	s, err := desync.NewLocalStore(storeLocation, sOpt)
	if err != nil {
		return err
	}

	// Start the server
	return desync.NewProtocolServer(os.Stdin, os.Stdout, s).Serve(ctx)
}
