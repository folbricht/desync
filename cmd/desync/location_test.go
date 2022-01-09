package main

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocationEquality(t *testing.T) {
	// Equal URLs
	require.True(t, locationMatch("http://host/path", "http://host/path"))
	require.True(t, locationMatch("http://host/path/", "http://host/path/"))
	require.True(t, locationMatch("http://host/path", "http://host/path/"))
	require.True(t, locationMatch("https://host/", "https://host"))
	require.True(t, locationMatch("https://host", "https://host/"))
	require.True(t, locationMatch("https://host", "https://host"))
	require.True(t, locationMatch("https://host/", "https://host/"))
	require.True(t, locationMatch("s3+https://example.com", "s3+https://example.com"))

	// Equal URLs with globs
	require.True(t, locationMatch("https://host/path*", "https://host/path"))
	require.True(t, locationMatch("https://host/path*", "https://host/path/"))
	require.True(t, locationMatch("https://*", "https://example.com"))
	require.True(t, locationMatch("https://example.com/path/*", "https://example.com/path/another"))
	require.True(t, locationMatch("https://example.com/path/*", "https://example.com/path/another/"))
	require.True(t, locationMatch("https://example.com/*/*/", "https://example.com/path/another/"))
	require.True(t, locationMatch("https://example.com/*/", "https://example.com/2022.01/"))
	require.True(t, locationMatch("https://*/*/*", "https://example.com/path/another/"))
	require.True(t, locationMatch("https://example.*", "https://example.com"))
	require.True(t, locationMatch("*://example.com", "https://example.com"))
	require.True(t, locationMatch("http*://example.com", "https://example.com"))
	require.True(t, locationMatch("http*://example.com", "http://example.com"))
	require.True(t, locationMatch("https://exampl?.*", "https://example.com"))
	require.True(t, locationMatch("http://examp??.com", "http://example.com"))
	require.True(t, locationMatch("https://example.com/?", "https://example.com/a"))
	require.True(t, locationMatch("https://example.com/fo[a-z]", "https://example.com/foo"))

	// Not equal URLs
	require.False(t, locationMatch("http://host:8080/path", "http://host/path"))
	require.False(t, locationMatch("http://host/path1", "http://host/path"))
	require.False(t, locationMatch("http://host/path1", "http://host/path/"))
	require.False(t, locationMatch("http://host1/path", "http://host2/path"))
	require.False(t, locationMatch("sftp://host/path", "http://host/path"))
	require.False(t, locationMatch("ssh://host/path", "/path"))
	require.False(t, locationMatch("ssh://host/path", "/host/path"))
	require.False(t, locationMatch("ssh://host/path", "/ssh/host/path"))

	// Not equal URLs with globs
	require.False(t, locationMatch("*", "https://example.com/path"))
	require.False(t, locationMatch("https://*", "https://example.com/path"))
	require.False(t, locationMatch("https://example.com/*", "https://example.com/path/another"))
	require.False(t, locationMatch("https://example.com/path/*", "https://example.com/path"))
	require.False(t, locationMatch("http://*", "https://example.com"))
	require.False(t, locationMatch("http?://example.com", "http://example.com"))
	require.False(t, locationMatch("https://example.com/123?", "https://example.com/12345"))
	require.False(t, locationMatch("*://example.com", "https://example.com/123"))

	// Equal paths
	require.True(t, locationMatch("/path", "/path/../path"))
	require.True(t, locationMatch("//path", "//path"))
	require.True(t, locationMatch("//path", "/path"))
	require.True(t, locationMatch("./path", "./path"))
	require.True(t, locationMatch("path", "path/"))
	require.True(t, locationMatch("path/..", "."))
	if runtime.GOOS == "windows" {
		require.True(t, locationMatch("c:\\path\\to\\somewhere", "c:\\path\\to\\somewhere\\"))
		require.True(t, locationMatch("/path/to/somewhere", "\\path\\to\\somewhere\\"))
	}

	// Equal paths with globs
	require.True(t, locationMatch("/path*", "/path/../path"))
	require.True(t, locationMatch("/path*", "/path_1"))
	require.True(t, locationMatch("/path/*", "/path/to"))
	require.True(t, locationMatch("/path/*", "/path/to/"))
	require.True(t, locationMatch("/path/*/", "/path/to/"))
	require.True(t, locationMatch("/path/*/", "/path/to"))
	require.True(t, locationMatch("/path/to/../*", "/path/another"))
	require.True(t, locationMatch("/*", "/path"))
	require.True(t, locationMatch("*", "path"))
	require.True(t, locationMatch("/pat?", "/path"))
	require.True(t, locationMatch("/pat?/?", "/path/1"))
	require.True(t, locationMatch("path/*", "path/to"))
	require.True(t, locationMatch("path/?", "path/1"))
	require.True(t, locationMatch("?", "a"))
	if runtime.GOOS == "windows" {
		require.True(t, locationMatch("c:\\path\\to\\*", "c:\\path\\to\\somewhere\\"))
		require.True(t, locationMatch("/path/to/*", "\\path\\to\\here\\"))
		require.True(t, locationMatch("c:\\path\\to\\?", "c:\\path\\to\\1\\"))
		require.True(t, locationMatch("/path/to/?", "\\path\\to\\1\\"))
	}

	// Not equal paths
	require.False(t, locationMatch("/path", "path"))
	require.False(t, locationMatch("/path/to", "path/to"))
	require.False(t, locationMatch("/path/to", "/path/to/.."))
	if runtime.GOOS == "windows" {
		require.False(t, locationMatch("c:\\path1", "c:\\path2"))
	}

	// Not equal paths with globs
	require.False(t, locationMatch("/path*", "/dir"))
	require.False(t, locationMatch("/path*", "path"))
	require.False(t, locationMatch("/path*", "/path/to"))
	require.False(t, locationMatch("/path/*", "/path"))
	require.False(t, locationMatch("/path/to/../*", "/path/to/another"))
	require.False(t, locationMatch("/pat?", "/pat"))
	require.False(t, locationMatch("/pat?", "/dir"))
	if runtime.GOOS == "windows" {
		require.True(t, locationMatch("c:\\path\\to\\*", "c:\\path\\to\\"))
		require.True(t, locationMatch("/path/to/*", "\\path\\to\\"))
		require.True(t, locationMatch("c:\\path\\to\\?", "c:\\path\\to\\123\\"))
		require.True(t, locationMatch("/path/to/?", "\\path\\to\\123\\"))
	}
}
