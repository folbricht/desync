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

	// Not equal URLs
	require.False(t, locationMatch("http://host:8080/path", "http://host/path"))
	require.False(t, locationMatch("http://host/path1", "http://host/path"))
	require.False(t, locationMatch("http://host/path1", "http://host/path/"))
	require.False(t, locationMatch("http://host1/path", "http://host2/path"))
	require.False(t, locationMatch("sftp://host/path", "http://host/path"))
	require.False(t, locationMatch("ssh://host/path", "/path"))

	// Equal paths
	require.True(t, locationMatch("/path", "/path/../path"))
	require.True(t, locationMatch("//path", "//path"))
	require.True(t, locationMatch("./path", "./path"))
	require.True(t, locationMatch("path", "path/"))
	require.True(t, locationMatch("path/..", "."))
	if runtime.GOOS == "windows" {
		require.True(t, locationMatch("c:\\path\\to\\somewhere", "c:\\path\\to\\somewhere\\"))
		require.True(t, locationMatch("/path/to/somewhere", "\\path\\to\\somewhere\\"))
	}

	// Not equal paths
	require.False(t, locationMatch("/path", "path"))
	require.False(t, locationMatch("/path/to", "path/to"))
	require.False(t, locationMatch("/path/to", "/path/to/.."))

}
