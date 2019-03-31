package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChopCommand(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"simple chop",
			[]string{"testdata/blob1.caibx", "testdata/blob1"}},
		{"chop with ignore",
			[]string{"--ignore", "testdata/blob2.caibx", "testdata/blob1.caibx", "testdata/blob1"}},
	} {
		store, err := ioutil.TempDir("", "")
		require.NoError(t, err)
		defer os.RemoveAll(store)

		args := []string{"-s", store}
		args = append(args, test.args...)

		cmd := newChopCommand(context.Background())
		cmd.SetArgs(args)

		// Redirect the command's output to turn off the progressbar and run it
		stderr = ioutil.Discard
		cmd.SetOutput(ioutil.Discard)
		_, err = cmd.ExecuteC()
		require.NoError(t, err)

		// If the file was split right, we'll have chunks in the dir now
		dirs, err := ioutil.ReadDir(store)
		require.NoError(t, err)
		require.NotEmpty(t, dirs)
	}
}

func TestChopErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"without store",
			[]string{"testdata/blob1.caibx", "testdata/blob1"}},
		{"invalid store",
			[]string{"-s", filepath.Join(os.TempDir(), "desync"), "testdata/blob1.caibx", "testdata/blob1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newChopCommand(context.Background())
			cmd.SetOutput(ioutil.Discard)
			cmd.SetArgs(test.args)
			_, err := cmd.ExecuteC()
			require.Error(t, err)
		})
	}
}
