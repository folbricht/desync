package main

import (
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

// Returns true if the two locations are equal. Locations can be URLs or local file paths.
// It can handle Unix as well as Windows paths. Example
// http://host/path/ is equal http://host/path (no trailing /) and /tmp/path is
// equal \tmp\path on Windows.
func locationMatch(pattern, loc string) bool {
	l, err := url.Parse(loc)
	if err != nil {
		return false
	}

	// See if we have a URL, Windows drive letters come out as single-letter
	// scheme, so we need more here.
	if len(l.Scheme) > 1 {
		// URL paths only use / as separator, so match with path.Match
		// rather than filepath.Match, whose separator is OS-dependent
		// (\ on Windows). Remove the trailing /, if any.
		trimmedLoc := strings.TrimSuffix(loc, "/")
		trimmedPattern := strings.TrimSuffix(pattern, "/")
		m, _ := path.Match(trimmedPattern, trimmedLoc)
		return m
	}

	// We're dealing with a path.
	p1, err := filepath.Abs(pattern)
	if err != nil {
		return false
	}
	p2, err := filepath.Abs(loc)
	if err != nil {
		return false
	}
	m, err := filepath.Match(p1, p2)
	if err != nil {
		return false
	}
	return m
}
