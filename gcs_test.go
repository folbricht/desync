package desync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeGCPrefix(t *testing.T) {

	tests := map[string]struct {
		path           string
		expectedPrefix string
	}{
		"blank path":                              {"", ""},
		"slash only":                              {"/", ""},
		"path with no slash":                      {"path", "path/"},
		"path with leading slash":                 {"/path", "path/"},
		"path with trailing slash":                {"path/", "path/"},
		"paths with no slashes":                   {"path1/path2", "path1/path2/"},
		"paths with leading slash":                {"/path1/path2", "path1/path2/"},
		"paths with trailing slash":               {"path1/path2/", "path1/path2/"},
		"paths with leading and trailing slashes": {"path1/path2/", "path1/path2/"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			prefix := normalizeGCPrefix(test.path)

			require.Equal(t, test.expectedPrefix, prefix)
		})
	}
}
