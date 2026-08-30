package main

import (
	"context"
	"os"

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
		Long:    `Generates man pages for desync and all of its commands into the given directory.`,
		Example: `  desync manpage /tmp/man`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runManpage(ctx, opt, root, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVar(&opt.Title, "title", "desync", "title")
	// Section 1 is user commands. Section 3 is library calls, which is where
	// these were being written, so an installed page went somewhere `man
	// desync` would not look.
	flags.StringVar(&opt.Section, "section", "1", "section")
	flags.StringVar(&opt.Source, "source", "desync", "source")
	flags.StringVar(&opt.Manual, "manual", "", "manual")
	return cmd
}

func runManpage(ctx context.Context, opt manpageOptions, root *cobra.Command, args []string) error {
	// cobra writes the pages but won't create the directory to put them in.
	if err := os.MkdirAll(args[0], 0755); err != nil {
		return err
	}
	return doc.GenManTree(root, &opt.GenManHeader, args[0])
}
