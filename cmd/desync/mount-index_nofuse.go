//go:build windows || netbsd || openbsd || dragonfly

package main

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
)

// newMountIndexCommand is a placeholder on platforms desync can't FUSE mount
// on. The command is registered (hidden) rather than omitted so that invoking
// it reports why it's unavailable instead of "unknown command".
func newMountIndexCommand(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:    "mount-index <index> <mountpoint>",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("command not available on this platform")
		},
		SilenceUsage: true,
	}
}
