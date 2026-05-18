package desync

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMtreeFilename(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"abc", "abc"},
		{"name with space", `name\040with\040space`},
		{`a\b`, `a\134b`},
		{"a#b", `a\043b`},
		{"a\tb", `a\011b`},
		{"a\nb", `a\012b`},
		{"a\x7fb", `a\177b`},
		{"é", `\303\251`}, // UTF-8 bytes, each > 126
	}
	for _, c := range cases {
		assert.Equal(t, c.want, mtreeFilename(c.in), "mtreeFilename(%q)", c.in)
	}
}

// TestMtreeFSNoKeywordInjection verifies that an entry name containing a space
// cannot inject mtree(5) keywords. mtree lines are space-delimited
// keyword=value pairs after the filename, so an unescaped space in
// "usr ignore" would make mtree(8) read "ignore" as a value-less keyword that
// suppresses verification of the whole subtree.
func TestMtreeFSNoKeywordInjection(t *testing.T) {
	var buf bytes.Buffer
	fs, err := NewMtreeFS(&buf)
	require.NoError(t, err)

	require.NoError(t, fs.CreateDir(NodeDirectory{
		Name:  "usr ignore",
		Mode:  os.ModeDir | 0755,
		MTime: time.Unix(0, 0),
	}))

	// Grab the line describing the directory (skip the "#mtree v1.0" header).
	var line string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(l, "#") || l == "" {
			continue
		}
		line = l
		break
	}
	require.NotEmpty(t, line)

	fields := strings.Split(line, " ")
	assert.Equal(t, `usr\040ignore`, fields[0], "filename must be a single escaped field")
	for _, f := range fields[1:] {
		assert.NotEqual(t, "ignore", f, "injected keyword must not appear as its own field")
	}
}
