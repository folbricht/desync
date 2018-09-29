package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatCommand(t *testing.T) {
	// Read the whole expected blob from disk
	f, err := ioutil.ReadFile("testdata/blob1")
	require.NoError(t, err)

	for _, test := range []struct {
		name           string
		args           []string
		offset, length int
	}{
		{"cat all data",
			[]string{"--store", "testdata/blob1.store", "testdata/blob1.caibx"}, 0, 0},
		{"cat with offset",
			[]string{"--store", "testdata/blob1.store", "-o", "1024", "testdata/blob1.caibx"}, 1024, 0},
		{"cat with offset and length",
			[]string{"--store", "testdata/blob1.store", "-o", "1024", "-l", "2048", "testdata/blob1.caibx"}, 1024, 2048},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newCatCommand(context.Background())
			cmd.SetArgs(test.args)
			b := new(bytes.Buffer)

			// Redirect the command's output
			stdout = b
			cmd.SetOutput(ioutil.Discard)
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			// Compare to what we should have gotten
			start := test.offset
			end := len(f)
			if test.length > 0 {
				end = start + test.length
			}
			require.Equal(t, f[start:end], b.Bytes())
		})
	}
}
