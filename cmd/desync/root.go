package main

import (
	"github.com/spf13/cobra"
)

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "desync",
		Short: "Content-addressed binary distribution system.",
	}
	cmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default $HOME/.config/desync/config.json)")
	return cmd
}

func init() {
	cobra.OnInitialize(initConfig)
}
