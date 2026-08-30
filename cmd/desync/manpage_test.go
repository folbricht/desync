package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// md2man, which cobra runs the man pages through, silently drops anything
// shaped like an HTML tag. Argument placeholders are shaped exactly like one,
// so without escaping they vanish from both the synopsis and any prose that
// names them, and nothing about the generated page looks wrong.
func TestManpagePlaceholders(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "man")

	root := newRootCommand()
	root.AddCommand(
		newChopCommand(ctx),
		newMtreeCommand(ctx),
		newManpageCommand(ctx, root),
	)
	root.SetArgs([]string{"manpage", dir})
	_, err := root.ExecuteC()
	require.NoError(t, err)

	chop, err := os.ReadFile(filepath.Join(dir, "desync-chop.1"))
	require.NoError(t, err)
	// The synopsis, and prose naming a flag's argument.
	require.Contains(t, string(chop), "desync chop <index> <file>")
	require.Contains(t, string(chop), "--ignore <index>")

	// Alternation has to survive too, and it isn't matched by a placeholder
	// pattern that only allows word characters.
	mtree, err := os.ReadFile(filepath.Join(dir, "desync-mtree.1"))
	require.NoError(t, err)
	require.Contains(t, string(mtree), "desync mtree <catar|index|dir>")

	// The escape must not reach the page. It would show up verbatim, and in a
	// code block it would break the command being demonstrated.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		require.NotContains(t, string(b), `\<`, "escape leaked into %s", e.Name())
	}
}

// Placeholders are only escaped for the duration of generation; the command
// tree is shared with the running CLI, where the escape would show up in
// --help output.
func TestManpageRestoresUse(t *testing.T) {
	ctx := context.Background()
	root := newRootCommand()
	chop := newChopCommand(ctx)
	root.AddCommand(chop, newManpageCommand(ctx, root))

	root.SetArgs([]string{"manpage", filepath.Join(t.TempDir(), "man")})
	_, err := root.ExecuteC()
	require.NoError(t, err)

	require.Equal(t, "chop <index> <file>", chop.Use)
	require.False(t, strings.Contains(chop.Long, `\<`))
}
