package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type listOptions struct {
	cmdStoreOptions
}

func newListCommand(ctx context.Context) *cobra.Command {
	var opt listOptions

	cmd := &cobra.Command{
		Use:   "list-chunks <index>",
		Short: "List chunk IDs from an index",
		Long: `Reads the index file and prints the list of chunk IDs in it. Use '-' to read
the index from STDIN.`,
		Example: `  desync list-chunks file.caibx`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.BoolVarP(&desync.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.StringVar(&opt.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&opt.clientKey, "client-key", "", "path to client key for TLS authentication")
	return cmd
}

func runList(ctx context.Context, opt listOptions, args []string) error {
	if (opt.clientKey == "") != (opt.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}

	// Read the input
	c, err := readCaibxFile(args[0], opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	// Write the list of chunk IDs to STDOUT
	for _, chunk := range c.Chunks {
		fmt.Fprintln(stdout, chunk.ID)
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	return nil
}
