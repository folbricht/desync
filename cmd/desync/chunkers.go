package main

import (
	"context"
	"fmt"

	"github.com/folbricht/desync/pkg/chunkers"
	"github.com/spf13/cobra"
)

func newChunkersCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chunkers",
		Short: "Lists available chunkers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChunkers(ctx)
		},
		SilenceUsage: true,
	}

	return cmd
}

func runChunkers(ctx context.Context) error {
	for _, name := range chunkers.RegisteredNames() {
		desc := chunkers.FindChunkerByName(name)
		fmt.Fprintf(stdout, "%s:\n", desc.Name)
		fmt.Fprintf(stdout, "WindowSize=%v Parallelizable=%v\n", desc.WindowSize, desc.Parallelizable)
		if len(desc.HelpText) > 0 {
			fmt.Fprint(stdout, desc.HelpText)
			if desc.HelpText[len(desc.HelpText)-1] != '\n' {
				fmt.Fprint(stdout, "\n")
			}
		}
		fmt.Fprint(stdout, "\n")
	}

	return nil
}
