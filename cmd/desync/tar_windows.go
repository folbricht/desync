// +build windows

package main

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
)

func newTarCommand(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("command not available on this platform")
		},
		SilenceUsage: true,
	}
}
