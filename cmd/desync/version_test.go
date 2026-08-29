package main

import (
	"bytes"
	"encoding/json"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// The version has to be reported even without link-time values, which is the
// case for 'go install' and for local builds, where it comes from the build
// information the toolchain embeds.
func TestVersionCommand(t *testing.T) {
	run := func(t *testing.T, args ...string) string {
		t.Helper()
		cmd := newVersionCommand()
		cmd.SetArgs(args)
		b := new(bytes.Buffer)
		stdout = b
		cmd.SetOutput(io.Discard)
		_, err := cmd.ExecuteC()
		require.NoError(t, err)
		return b.String()
	}

	require.Contains(t, run(t), "desync version ")

	var got buildInfo
	require.NoError(t, json.Unmarshal([]byte(run(t, "--format=json")), &got))
	require.NotEmpty(t, got.Version)
	require.NotEqual(t, "unknown", got.Version, "expected a version from the embedded build info")
	require.Equal(t, runtime.Version(), got.GoVersion)
	require.Equal(t, runtime.GOOS, got.OS)
	require.Equal(t, runtime.GOARCH, got.Arch)
}
