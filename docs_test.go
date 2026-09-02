package desync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	markdownLink = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)\)`)
	markdownHead = regexp.MustCompile(`(?m)^#{1,6} (.+)$`)
	anchorDrop   = regexp.MustCompile(`[^a-z0-9 _-]`)
)

// anchorsIn returns the fragment identifiers GitHub generates for a file's
// headings, skipping fenced code blocks so a comment starting with # isn't
// mistaken for one.
func anchorsIn(t *testing.T, path string) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)

	out := map[string]bool{}
	var fenced bool
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "```") {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		m := markdownHead.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		a := anchorDrop.ReplaceAllString(strings.ToLower(m[1]), "")
		out[strings.ReplaceAll(strings.TrimSpace(a), " ", "-")] = true
	}
	return out
}

// The documentation is a set of cross-linked files, so renaming a page or
// moving a section can break a link without breaking anything a compiler or
// the other tests would notice.
func TestDocumentationLinks(t *testing.T) {
	var files []string
	require.NoError(t, filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "testdata" || d.Name() == "vendor") {
			return filepath.SkipDir
		}
		if !d.IsDir() && filepath.Ext(path) == ".md" {
			files = append(files, path)
		}
		return nil
	}))
	require.NotEmpty(t, files)

	anchors := map[string]map[string]bool{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		for _, m := range markdownLink.FindAllStringSubmatch(string(b), -1) {
			link := m[1]
			if strings.Contains(link, "://") || strings.HasPrefix(link, "mailto:") {
				continue
			}
			path, frag, _ := strings.Cut(link, "#")

			target := f
			if path != "" {
				target = filepath.Join(filepath.Dir(f), path)
				if fi, err := os.Stat(target); err == nil && fi.IsDir() {
					target = filepath.Join(target, "README.md")
				}
				_, err := os.Stat(target)
				require.NoErrorf(t, err, "%s links to %s, which does not exist", f, link)
			}
			if frag == "" {
				continue
			}
			if _, ok := anchors[target]; !ok {
				anchors[target] = anchorsIn(t, target)
			}
			require.Truef(t, anchors[target][frag], "%s links to %s, but %s has no such heading", f, link, target)
		}
	}
}
