package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtracCommand(t *testing.T) {
	// Read the whole expected blob from disk
	expected, err := ioutil.ReadFile("testdata/blob1")
	require.NoError(t, err)

	// Now prepare several files used to extract into
	outDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)
	out1 := filepath.Join(outDir, "out1") // Doesn't exit
	out2 := filepath.Join(outDir, "out2") // Exists, but different content
	require.NoError(t, ioutil.WriteFile(out2, []byte{0, 1, 2, 3}, 0644))
	out3 := filepath.Join(outDir, "out3") // Exist and complete match
	require.NoError(t, ioutil.WriteFile(out3, expected, 0644))

	// Make a cache dir
	cacheDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(cacheDir)

	for _, test := range []struct {
		name   string
		args   []string
		output string
	}{
		{"extract to new file",
			[]string{"--store", "testdata/blob1.store", "testdata/blob1.caibx"}, out1},
		{"extract to exiting file with overwrite",
			[]string{"--store", "testdata/blob1.store", "testdata/blob1.caibx"}, out2},
		{"extract to exiting file without overwrite", // no need for a store here, data is in the file
			[]string{"--in-place", "--store", outDir, "testdata/blob1.caibx"}, out3},
		{"extract with single seed",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2.caibx", "testdata/blob1.caibx"}, out1},
		{"extract with multi seed",
			[]string{"-s", "testdata/blob1.store", "--seed", "testdata/blob2.caibx", "--seed", "testdata/blob1.caibx", "testdata/blob1.caibx"}, out1},
		{"extract with seed directory",
			[]string{"-s", "testdata/blob1.store", "--seed-dir", "testdata", "testdata/blob1.caibx"}, out1},
		{"extract with cache",
			[]string{"-s", "testdata/blob1.store", "-c", cacheDir, "testdata/blob1.caibx"}, out1},
		{"extract with multiple stores",
			[]string{"-s", "testdata/blob2.store", "-s", "testdata/blob1.store", "testdata/blob1.caibx"}, out1},
		{"extract with multiple stores and cache",
			[]string{"-n", "1", "-s", "testdata/blob2.store", "-s", "testdata/blob1.store", "--cache", cacheDir, "testdata/blob1.caibx"}, out1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newExtractCommand(context.Background())
			cmd.SetArgs(append(test.args, test.output))

			// Redirect the command's output and run it
			stderr = ioutil.Discard
			cmd.SetOutput(ioutil.Discard)
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			// Compare to what we should have gotten
			got, err := ioutil.ReadFile(test.output)
			require.NoError(t, err)
			require.Equal(t, expected, got)
		})
	}
}
