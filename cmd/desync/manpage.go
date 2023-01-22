package main

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

type manpageOptions struct {
	doc.GenManHeader
}

func newManpageCommand(ctx context.Context, root *cobra.Command) *cobra.Command {
	var opt manpageOptions

	cmd := &cobra.Command{
		Use:     "manpage <output-directory>",
		Short:   "Generate manpages for desync",
		Example: `  desync manpage /tmp/man`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runManpage(ctx, opt, root, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVar(&opt.Title, "title", "desync", "title")
	flags.StringVar(&opt.Section, "section", "3", "section")
	flags.StringVar(&opt.Source, "source", "", "source")
	flags.StringVar(&opt.Manual, "manual", "", "manual")
	return cmd
}

func runManpage(ctx context.Context, opt manpageOptions, root *cobra.Command, args []string) error {
	return doc.GenManTree(root, &opt.GenManHeader, args[0])
}
