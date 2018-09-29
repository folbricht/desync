package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexServerReadCommand(t *testing.T) {
	// Start a read-only server
	addr, cancel := startIndexServer(t, "-s", "testdata")
	defer cancel()

	// Run a "list-chunks" command on a valid index to confirm it can be read
	listCmd := newListCommand(context.Background())
	listCmd.SetArgs([]string{fmt.Sprintf("http://%s/blob1.caibx", addr)})
	stdout = ioutil.Discard
	listCmd.SetOutput(ioutil.Discard)
	_, err := listCmd.ExecuteC()
	require.NoError(t, err)

	// The index server should not be serving up arbitrary files from disk even if
	// they're in the store. Try to HTTP GET a non-index file expecting a 400 from
	// the server.
	resp, err := http.Get(fmt.Sprintf("http://%s/blob1", addr))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// This server shouldn't allow writing. Confirm by trying to chunk a file with
	// the "make" command and storing a new index on the index server.
	makeCmd := newMakeCommand(context.Background())
	makeCmd.SetArgs([]string{fmt.Sprintf("http://%s/new.caibx", addr), "testdata/blob1"})
	makeCmd.SetOutput(ioutil.Discard)
	_, err = makeCmd.ExecuteC()
	require.Error(t, err)
	require.Contains(t, err.Error(), "writing to upstream")
}

func TestIndexServerWriteCommand(t *testing.T) {
	// Create an empty store to be used for writing
	store, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(store)

	// Start a read-write server
	addr, cancel := startIndexServer(t, "-s", store, "-w")
	defer cancel()

	// This server should allow writing. Confirm by trying to chunk a file with
	// the "make" command and storing a new index on the index server.
	makeCmd := newMakeCommand(context.Background())
	makeCmd.SetArgs([]string{fmt.Sprintf("http://%s/new.caibx", addr), "testdata/blob1"})
	makeCmd.SetOutput(ioutil.Discard)
	_, err = makeCmd.ExecuteC()
	require.NoError(t, err)

	// The index server should not accept arbitrary (non-index) files.
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/invalid.caibx", addr), strings.NewReader("invalid"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
}

func startIndexServer(t *testing.T, args ...string) (string, context.CancelFunc) {
	// Find a free local port to be used to run the index server on
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	// Flush any handlers that were registered in the default mux before
	http.DefaultServeMux = &http.ServeMux{}

	// Start the server in a gorountine. Cancel the context when done
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newIndexServerCommand(ctx)
	cmd.SetArgs(append(args, "-l", addr))
	go func() {
		_, err = cmd.ExecuteC()
		require.NoError(t, err)
	}()

	// Wait a little for the server to start
	time.Sleep(time.Second)
	return addr, cancel
}
