package main

import (
	"context"
	"errors"

	"github.com/spf13/cobra"

	"github.com/folbricht/desync"
)

type verifyOptions struct {
	cmdStoreOptions
	store  string
	repair bool
}

func newVerifyCommand(ctx context.Context) *cobra.Command {
	var opt verifyOptions

	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Read chunks in a store and verify their integrity",
		Long: `Reads all chunks in a local store and verifies their integrity. If -r is used,
invalid chunks are deleted from the store.`,
		Example: `  desync verify -s /path/to/store`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVerify(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store")
	flags.IntVarP(&opt.n, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.BoolVarP(&opt.repair, "repair", "r", false, "remove invalid chunks from the store")
	return cmd
}

func runVerify(ctx context.Context, opt verifyOptions, args []string) error {
	if opt.store == "" {
		return errors.New("no store provided")
	}
	options, err := cfg.GetStoreOptionsFor(opt.store)
	if err != nil {
		return err
	}
	s, err := desync.NewLocalStore(opt.store, options)
	if err != nil {
		return err
	}
	return s.Verify(ctx, opt.n, opt.repair, stderr)
}
