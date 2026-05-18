package main

// TEMPORARY diagnostic — not for merge. Evaluates every locationMatch case
// from TestLocationEquality (including the Windows-only blocks) without
// short-circuiting, and prints the path-branch internals, so a single
// Windows CI run reveals the complete set of divergences and their causes.

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLocationDiag(t *testing.T) {
	type c struct {
		pattern, loc string
		want         bool
		group        string
	}
	cases := []c{
		// Equal URLs
		{"http://host/path", "http://host/path", true, "eq-url"},
		{"http://host/path/", "http://host/path/", true, "eq-url"},
		{"http://host/path", "http://host/path/", true, "eq-url"},
		{"https://host/", "https://host", true, "eq-url"},
		{"https://host", "https://host/", true, "eq-url"},
		{"https://host", "https://host", true, "eq-url"},
		{"https://host/", "https://host/", true, "eq-url"},
		{"s3+https://example.com", "s3+https://example.com", true, "eq-url"},
		// Equal URLs with globs
		{"https://host/path*", "https://host/path", true, "eq-url-glob"},
		{"https://host/path*", "https://host/path/", true, "eq-url-glob"},
		{"https://*", "https://example.com", true, "eq-url-glob"},
		{"https://example.com/path/*", "https://example.com/path/another", true, "eq-url-glob"},
		{"https://example.com/path/*", "https://example.com/path/another/", true, "eq-url-glob"},
		{"https://example.com/*/*/", "https://example.com/path/another/", true, "eq-url-glob"},
		{"https://example.com/*/", "https://example.com/2022.01/", true, "eq-url-glob"},
		{"https://*/*/*", "https://example.com/path/another/", true, "eq-url-glob"},
		{"https://example.*", "https://example.com", true, "eq-url-glob"},
		{"*://example.com", "https://example.com", true, "eq-url-glob"},
		{"http*://example.com", "https://example.com", true, "eq-url-glob"},
		{"http*://example.com", "http://example.com", true, "eq-url-glob"},
		{"https://exampl?.*", "https://example.com", true, "eq-url-glob"},
		{"http://examp??.com", "http://example.com", true, "eq-url-glob"},
		{"https://example.com/?", "https://example.com/a", true, "eq-url-glob"},
		{"https://example.com/fo[a-z]", "https://example.com/foo", true, "eq-url-glob"},
		// Not equal URLs
		{"http://host:8080/path", "http://host/path", false, "ne-url"},
		{"http://host/path1", "http://host/path", false, "ne-url"},
		{"http://host/path1", "http://host/path/", false, "ne-url"},
		{"http://host1/path", "http://host2/path", false, "ne-url"},
		{"sftp://host/path", "http://host/path", false, "ne-url"},
		{"ssh://host/path", "/path", false, "ne-url"},
		{"ssh://host/path", "/host/path", false, "ne-url"},
		{"ssh://host/path", "/ssh/host/path", false, "ne-url"},
		// Not equal URLs with globs
		{"*", "https://example.com/path", false, "ne-url-glob"},
		{"https://*", "https://example.com/path", false, "ne-url-glob"},
		{"https://example.com/*", "https://example.com/path/another", false, "ne-url-glob"},
		{"https://example.com/path/*", "https://example.com/path", false, "ne-url-glob"},
		{"http://*", "https://example.com", false, "ne-url-glob"},
		{"http?://example.com", "http://example.com", false, "ne-url-glob"},
		{"https://example.com/123?", "https://example.com/12345", false, "ne-url-glob"},
		{"*://example.com", "https://example.com/123", false, "ne-url-glob"},
		// Equal paths
		{"/path", "/path/../path", true, "eq-path"},
		{"//path", "//path", true, "eq-path"},
		{"//path", "/path", true, "eq-path"},
		{"./path", "./path", true, "eq-path"},
		{"path", "path/", true, "eq-path"},
		{"path/..", ".", true, "eq-path"},
		// Equal paths (Windows-only block)
		{"c:\\path\\to\\somewhere", "c:\\path\\to\\somewhere\\", true, "eq-path-win"},
		{"/path/to/somewhere", "\\path\\to\\somewhere\\", true, "eq-path-win"},
		// Not equal paths
		{"/path", "path", false, "ne-path"},
		{"/path/to", "path/to", false, "ne-path"},
		{"/path/to", "/path/to/..", false, "ne-path"},
		// Not equal paths (Windows-only block)
		{"c:\\path1", "c:\\path2", false, "ne-path-win"},
		// Not equal paths with globs
		{"/path*", "/dir", false, "ne-path-glob"},
		{"/path*", "path", false, "ne-path-glob"},
		{"/path*", "/path/to", false, "ne-path-glob"},
		{"/path/*", "/path", false, "ne-path-glob"},
		{"/path/to/../*", "/path/to/another", false, "ne-path-glob"},
		{"/pat?", "/pat", false, "ne-path-glob"},
		{"/pat?", "/dir", false, "ne-path-glob"},
		// "Not equal paths with globs" Windows-only block (asserts True)
		{"c:\\path\\to\\*", "c:\\path\\to\\", true, "win-glob"},
		{"/path/to/*", "\\path\\to\\", true, "win-glob"},
		{"c:\\path\\to\\?", "c:\\path\\to\\123\\", true, "win-glob"},
		{"/path/to/?", "\\path\\to\\123\\", true, "win-glob"},
	}

	t.Logf("GOOS=%s  total=%d", runtime.GOOS, len(cases))
	fails := 0
	for i, tc := range cases {
		got := locationMatch(tc.pattern, tc.loc)
		status := "ok"
		if got != tc.want {
			status = "FAIL"
			fails++
		}
		// Recompute the branch internals for visibility.
		detail := ""
		if u, err := url.Parse(tc.loc); err == nil && len(u.Scheme) > 1 {
			tp := strings.TrimSuffix(tc.pattern, "/")
			tl := strings.TrimSuffix(tc.loc, "/")
			pm, _ := path.Match(tp, tl)
			detail = fmt.Sprintf("URL  scheme=%q path.Match(%q,%q)=%v", u.Scheme, tp, tl, pm)
		} else {
			ap, _ := filepath.Abs(tc.pattern)
			al, _ := filepath.Abs(tc.loc)
			fm, fmErr := filepath.Match(ap, al)
			detail = fmt.Sprintf("PATH Abs(p)=%q Abs(l)=%q filepath.Match=%v err=%v", ap, al, fm, fmErr)
		}
		t.Logf("[%2d] %-12s %-4s want=%-5v got=%-5v | p=%q l=%q | %s",
			i, tc.group, status, tc.want, got, tc.pattern, tc.loc, detail)
	}
	t.Logf("SUMMARY: %d/%d failed on %s", fails, len(cases), runtime.GOOS)
	if fails > 0 {
		t.Errorf("%d locationMatch divergences on %s (see table above)", fails, runtime.GOOS)
	}
}
