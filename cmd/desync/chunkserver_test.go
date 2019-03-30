package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChunkServerReadCommand(t *testing.T) {
	// Start a read-only server
	addr, cancel := startChunkServer(t, "-s", "testdata/blob1.store")
	defer cancel()
	store := fmt.Sprintf("http://%s/", addr)

	outdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outdir)
	blob := filepath.Join(outdir, "blob")

	// Run an "extract" command to confirm the chunk server provides chunks
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", blob})
	stdout = ioutil.Discard
	extractCmd.SetOutput(ioutil.Discard)
	_, err = extractCmd.ExecuteC()
	require.NoError(t, err)

	// The server should not be serving up arbitrary files from disk. Expect a 400 error
	resp, err := http.Get(store + "somefile")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Asking for a chunk that doesn't exist should return 404
	resp, err = http.Get(store + "0000/0000000000000000000000000000000000000000000000000000000000000000.cacnk")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// This server shouldn't allow writing. Confirm by trying to chunk a file with
	// the "chop" command and storing the chunks there.
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob2.caibx", "testdata/blob2"})
	chopCmd.SetOutput(ioutil.Discard)
	_, err = chopCmd.ExecuteC()
	require.Error(t, err)
	require.Contains(t, err.Error(), "writing to upstream")
}

func TestChunkServerWriteCommand(t *testing.T) {
	// Create a blank store
	outdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outdir)

	// Start a (writable) server
	addr, cancel := startChunkServer(t, "-s", outdir, "-w")
	defer cancel()
	store := fmt.Sprintf("http://%s/", addr)

	// Run a "chop" command to confirm the chunk server can be used to write chunks
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	chopCmd.SetOutput(ioutil.Discard)
	_, err = chopCmd.ExecuteC()
	require.NoError(t, err)

	// The server should not accept arbitrary (non-chunk) files.
	req, _ := http.NewRequest("PUT", store+"somefile", strings.NewReader("invalid"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
func TestChunkServerVerifiedTLS(t *testing.T) {
	outdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outdir)

	// Start a (writable) server
	addr, cancel := startChunkServer(t, "-s", "testdata/blob1.store", "--key", "testdata/server.key", "--cert", "testdata/server.crt")
	defer cancel()
	_, port, _ := net.SplitHostPort(addr)
	store := fmt.Sprintf("https://localhost:%s/", port)

	// Run the "extract" command to confirm the TLS chunk server can be used
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"--ca-cert", "testdata/ca.crt", "-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	extractCmd.SetOutput(ioutil.Discard)
	_, err = extractCmd.ExecuteC()
	require.NoError(t, err)
}

func TestChunkServerInsecureTLS(t *testing.T) {
	outdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outdir)

	stderr = ioutil.Discard
	stdout = ioutil.Discard

	// Start a (writable) server
	addr, cancel := startChunkServer(t, "-s", "testdata/blob1.store", "--key", "testdata/server.key", "--cert", "testdata/server.crt")
	defer cancel()
	_, port, _ := net.SplitHostPort(addr)
	store := fmt.Sprintf("https://localhost:%s/", port)

	// Run the "extract" command accepting any cert  to confirm the TLS chunk server can be used
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"-t", "-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	// extractCmd.SetOutput(ioutil.Discard)
	_, err = extractCmd.ExecuteC()
	require.NoError(t, err)

	// Run the "extract" command without accepting any cert. Should fail.
	extractCmd = newExtractCommand(context.Background())
	extractCmd.SetOutput(ioutil.Discard)
	extractCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	extractCmd.SetOutput(ioutil.Discard)
	_, err = extractCmd.ExecuteC()
	require.Error(t, err)

}

func TestChunkServerMutualTLS(t *testing.T) {
	outdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(outdir)

	stderr = ioutil.Discard
	stdout = ioutil.Discard

	// Start a (writable) server
	addr, cancel := startChunkServer(t,
		"-s", "testdata/blob1.store",
		"--mutual-tls",
		"--key", "testdata/server.key",
		"--cert", "testdata/server.crt",
		"--client-ca", "testdata/ca.crt",
	)
	defer cancel()
	_, port, _ := net.SplitHostPort(addr)
	store := fmt.Sprintf("https://localhost:%s/", port)

	// Run the "extract" command to confirm the TLS chunk server can be used
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{
		"--client-key", "testdata/client.key",
		"--client-cert", "testdata/client.crt",
		"--ca-cert", "testdata/ca.crt",
		"-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	_, err = extractCmd.ExecuteC()
	require.NoError(t, err)

	// Same without client certs, should fail.
	extractCmd = newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{
		"--ca-cert", "testdata/ca.crt",
		"-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	extractCmd.SetOutput(ioutil.Discard)
	_, err = extractCmd.ExecuteC()
	require.Error(t, err)
}

func startChunkServer(t *testing.T, args ...string) (string, context.CancelFunc) {
	// Find a free local port to be used to run the index server on
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	// Flush any handlers that were registered in the default mux before
	http.DefaultServeMux = &http.ServeMux{}

	// Start the server in a gorountine. Cancel the context when done
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newChunkServerCommand(ctx)
	cmd.SetArgs(append(args, "-l", addr))
	go func() {
		_, err = cmd.ExecuteC()
		require.NoError(t, err)
	}()

	// Wait a little for the server to start
	time.Sleep(time.Second)
	return addr, cancel
}
