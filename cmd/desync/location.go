package main

import (
	"net/url"
	"path"
	"path/filepath"
)

// Returns true if the two locations are equal. Locations can be URLs or local file paths.
// It can handle Unix as well as Windows paths. Example
// http://host/path/ is equal http://host/path (no trailing /) and /tmp/path is
// equal \tmp\path on Windows.
func locationMatch(loc1, loc2 string) bool {
	u1, _ := url.Parse(loc1)
	u2, _ := url.Parse(loc2)
	// See if we have at least one URL, Windows drive letters come out as single-letter
	// scheme so we need more here.
	if len(u1.Scheme) > 1 || len(u2.Scheme) > 1 {
		if u1.Scheme != u2.Scheme || u1.Host != u2.Host {
			return false
		}
		// URL paths should only use /, use path (not filepath) package to clean them
		// before comparing
		return path.Clean(u1.Path) == path.Clean(u2.Path)
	}

	// We're dealing with two paths.
	p1, err := filepath.Abs(loc1)
	if err != nil {
		return false
	}
	p2, err := filepath.Abs(loc2)
	if err != nil {
		return false
	}
	return p1 == p2
}
