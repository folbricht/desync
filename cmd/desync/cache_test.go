package main

import (
	"context"
	"io/ioutil"
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
			cache, err := ioutil.TempDir("", "")
			require.NoError(t, err)
			defer os.RemoveAll(cache)

			cmd := newCacheCommand(context.Background())
			cmd.SetArgs(append(test.args, "-c", cache))

			// Redirect the command's output to turn off the progressbar and run it
			stderr = ioutil.Discard
			cmd.SetOutput(ioutil.Discard)
			_, err = cmd.ExecuteC()
			require.NoError(t, err)

			// If the file was split right, we'll have chunks in the dir now
			dirs, err := ioutil.ReadDir(cache)
			require.NoError(t, err)
			require.NotEmpty(t, dirs)
		})
	}
}
