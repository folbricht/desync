package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

// newGendocsCommand generates the markdown CLI reference under docs/cli. It's
// hidden because it maintains this repository's documentation rather than doing
// anything for someone using the tool. CI regenerates and checks the result
// matches what's committed, which is what stops the reference from drifting
// away from the flags the code actually defines.
func newGendocsCommand(root *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "gendocs <output-directory>",
		Short:   "Generate the markdown CLI reference",
		Long:    `Generates a markdown page per command into the given directory.`,
		Example: `  desync gendocs docs/cli`,
		Args:    cobra.ExactArgs(1),
		Hidden:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGendocs(root, args[0])
		},
		SilenceUsage: true,
	}
	return cmd
}

func runGendocs(root *cobra.Command, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// Drop what's there first, otherwise the page for a removed command
	// survives regeneration and the freshness check has nothing to catch.
	existing, err := filepath.Glob(filepath.Join(dir, "desync*.md"))
	if err != nil {
		return err
	}
	for _, f := range existing {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	// Without this every page carries the date it was written, so the
	// freshness check would fail a day after any change.
	root.DisableAutoGenTag = true
	return doc.GenMarkdownTree(root, dir)
}
