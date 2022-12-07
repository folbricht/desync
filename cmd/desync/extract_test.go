package main

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractCommand(t *testing.T) {
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
			[]string{"-s", "testdata/blob1.store", "--seed-dir", "testdata", "--skip-invalid-seeds", "testdata/blob1.caibx"}, out1},
		{"extract with single seed and explicit data directory",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2_without_data.caibx:testdata/blob2", "testdata/blob1.caibx"}, out1},
		{"extract with single seed, explicit data directory and unexpected seed options",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2_without_data.caibx:testdata/blob2:reserved_options", "testdata/blob1.caibx"}, out1},
		{"extract with multi seed and explicit data directories",
			[]string{"-s", "testdata/blob1.store", "--seed", "testdata/blob2_without_data.caibx:testdata/blob2", "--seed", "testdata/blob1_without_data.caibx:testdata/blob1", "testdata/blob1.caibx"}, out1},
		{"extract with multi seed and one explicit data directory",
			[]string{"-s", "testdata/blob1.store", "--seed", "testdata/blob2_without_data.caibx:testdata/blob2", "--seed", "testdata/blob1.caibx", "testdata/blob1.caibx"}, out1},
		{"extract with cache",
			[]string{"-s", "testdata/blob1.store", "-c", cacheDir, "testdata/blob1.caibx"}, out1},
		{"extract with multiple stores",
			[]string{"-s", "testdata/blob2.store", "-s", "testdata/blob1.store", "testdata/blob1.caibx"}, out1},
		{"extract with multiple stores and cache",
			[]string{"-n", "1", "-s", "testdata/blob2.store", "-s", "testdata/blob1.store", "--cache", cacheDir, "testdata/blob1.caibx"}, out1},
		{"extract with corrupted seed",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2_corrupted.caibx", "--skip-invalid-seeds", "testdata/blob1.caibx"}, out1},
		{"extract with multiple corrupted seeds",
			[]string{"--store", "testdata/empty.store", "--seed", "testdata/blob2_corrupted.caibx", "--seed", "testdata/blob1.caibx", "--skip-invalid-seeds", "testdata/blob1.caibx"}, out1},
		// Here we don't need the `--skip-invalid-seeds` because we expect the blob1 seed to always be the chosen one, being
		// a 1:1 match with the index that we want to write. So we never reach the point where we validate the corrupted seed.
		// Explicitly set blob1 seed because seed-dir skips a seed if it's the same index file we gave in input.
		{"extract with seed directory without skipping invalid seeds",
			[]string{"-s", "testdata/blob1.store", "--seed-dir", "testdata", "--seed", "testdata/blob1.caibx", "testdata/blob1.caibx"}, out1},
		// Same as above, no need for `--skip-invalid-seeds`
		{"extract with multiple corrupted seeds",
			[]string{"--store", "testdata/empty.store", "--seed", "testdata/blob2_corrupted.caibx", "--seed", "testdata/blob1.caibx", "testdata/blob1.caibx"}, out1},
		{"extract with single seed that has all the expected chunks",
			[]string{"--store", "testdata/empty.store", "--seed", "testdata/blob1.caibx", "testdata/blob1.caibx"}, out1},
		// blob2_corrupted is a corrupted blob that doesn't match its seed index. We regenerate the seed index to match
		// this corrupted blob
		{"extract while regenerating the corrupted seed",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2_corrupted.caibx", "--regenerate-invalid-seeds", "testdata/blob1.caibx"}, out1},
		// blob1_corrupted_index.caibx is a corrupted seed index that points to a valid blob1 file. By regenerating the
		// invalid seed we expect to have an index that is equal to blob1.caibx. That should be enough to do the
		// extraction without taking chunks from the store
		{"extract with corrupted seed and empty store",
			[]string{"--store", "testdata/empty.store", "--seed", "testdata/blob1_corrupted_index.caibx", "--regenerate-invalid-seeds", "testdata/blob1.caibx"}, out1},
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

func TestExtractWithFailover(t *testing.T) {
	outDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)
	out := filepath.Join(outDir, "out")

	// Start a server that'll always fail
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer ts.Close()

	// Use the HTTP server to simulate a failing store. It should fail over to the local store and succeed
	cmd := newExtractCommand(context.Background())
	cmd.SetArgs([]string{"--store", ts.URL + "|testdata/blob1.store", "testdata/blob1.caibx", out})

	// Redirect the command's output and run it
	stderr = ioutil.Discard
	cmd.SetOutput(ioutil.Discard)
	_, err = cmd.ExecuteC()
	require.NoError(t, err)
}

func TestExtractWithInvalidSeeds(t *testing.T) {
	outDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)
	out := filepath.Join(outDir, "out")

	for _, test := range []struct {
		name   string
		args   []string
		output string
	}{
		{"extract with corrupted seed",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2_corrupted.caibx", "testdata/blob1.caibx"}, out},
		{"extract with missing seed",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob_missing", "testdata/blob1.caibx"}, out},
		{"extract with missing seed data",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob2_without_data.caibx", "testdata/blob1.caibx"}, out},
		{"extract with multiple corrupted seeds",
			[]string{"--store", "testdata/empty.store", "--seed", "testdata/blob2_corrupted.caibx", "--seed", "testdata/blob1.caibx", "testdata/blob2.caibx"}, out},
		{"extract with corrupted blob1 seed and a valid seed",
			[]string{"--store", "testdata/blob2.store", "--seed", "testdata/blob1_corrupted_index.caibx", "--seed", "testdata/blob1.caibx", "testdata/blob2.caibx"}, out},
		{"extract with corrupted blob1 seed",
			[]string{"--store", "testdata/blob2.store", "--seed", "testdata/blob1_corrupted_index.caibx", "testdata/blob2.caibx"}, out},
		{"extract with both --regenerate-invalid-seed and --skip-invalid-seeds",
			[]string{"--store", "testdata/blob1.store", "--seed", "testdata/blob1_corrupted_index.caibx", "--regenerate-invalid-seeds", "--skip-invalid-seeds", "testdata/blob1.caibx"}, out},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newExtractCommand(context.Background())
			cmd.SetArgs(append(test.args, test.output))

			// Redirect the command's output and run it
			stderr = ioutil.Discard
			cmd.SetOutput(ioutil.Discard)
			_, err := cmd.ExecuteC()
			require.Error(t, err)
		})
	}
}
