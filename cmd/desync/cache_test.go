package main

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheCommand(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"singe store, single index",
			[]string{"--store", "testdata/blob1.store", "testdata/blob1.caibx"}},
		{"multiple store, single index",
			[]string{"--store", "testdata/blob1.store", "--store", "testdata/blob2.store", "testdata/blob1.caibx"}},
		{"multiple store, multiple index",
			[]string{"--store", "testdata/blob1.store", "--store", "testdata/blob2.store", "testdata/blob1.caibx", "testdata/blob2.caibx"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := t.TempDir()

			cmd := newCacheCommand(context.Background())
			cmd.SetArgs(append(test.args, "-c", cache))

			// Redirect the command's output to turn off the progressbar and run it
			stderr = io.Discard
			cmd.SetOutput(io.Discard)
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			// If the file was split right, we'll have chunks in the dir now
			dirs, err := os.ReadDir(cache)
			require.NoError(t, err)
			require.NotEmpty(t, dirs)
		})
	}
}
