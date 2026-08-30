package main

import (
	"context"
	"os"
	"regexp"

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
	defer escapePlaceholders(root)()
	return doc.GenManTree(root, &opt.GenManHeader, args[0])
}

// placeholder matches an argument placeholder such as <index>, and nothing
// else that opens with an angle bracket. Shell redirection and process
// substitution, which appear in the completion command's own help text, must
// be left alone: they sit inside code blocks where an escape would survive
// into the output and break the command being demonstrated.
var placeholder = regexp.MustCompile(`<[A-Za-z][A-Za-z0-9_.|-]*>`)

// escapePlaceholders escapes the angle brackets of argument placeholders in
// the command descriptions for the duration of man page generation, and
// returns a function restoring them.
//
// cobra builds each page as markdown and runs it through md2man. That parses
// <index> as an inline HTML tag and drops it, so "desync chop <index> <file>"
// renders as "desync chop", and prose like "use --ignore <index> which will"
// loses the argument mid-sentence. Escaping the bracket makes the markdown
// parser emit it as literal text instead. Only the synopsis and description
// need it; examples already sit in a code fence, which md2man leaves alone.
func escapePlaceholders(root *cobra.Command) func() {
	escape := func(s string) string {
		return placeholder.ReplaceAllStringFunc(s, func(m string) string { return `\` + m })
	}
	var restore []func()
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		use, short, long := c.Use, c.Short, c.Long
		restore = append(restore, func() { c.Use, c.Short, c.Long = use, short, long })
		c.Use, c.Short, c.Long = escape(c.Use), escape(c.Short), escape(c.Long)
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
	return func() {
		for _, f := range restore {
			f()
		}
	}
}
