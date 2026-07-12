package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
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
	outdir := t.TempDir()

	// Start a read-only server
	addr, cancel := startChunkServer(t, "-s", "testdata/blob1.store")
	defer cancel()
	store := fmt.Sprintf("http://%s/", addr)

	// Run an "extract" command to confirm the chunk server provides chunks
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob")})
	stdout = io.Discard
	extractCmd.SetOutput(io.Discard)
	_, err := extractCmd.ExecuteC()
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
	chopCmd.SetOutput(io.Discard)
	_, err = chopCmd.ExecuteC()
	require.Error(t, err)
	require.Contains(t, err.Error(), "writing to upstream")
}

func TestChunkServerWriteCommand(t *testing.T) {
	outdir := t.TempDir()

	// Start a (writable) server
	addr, cancel := startChunkServer(t, "-s", outdir, "-w")
	defer cancel()
	store := fmt.Sprintf("http://%s/", addr)

	// Run a "chop" command to confirm the chunk server can be used to write chunks
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	chopCmd.SetOutput(io.Discard)
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// The server should not accept arbitrary (non-chunk) files.
	req, _ := http.NewRequest("PUT", store+"somefile", strings.NewReader("invalid"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
func TestChunkServerVerifiedTLS(t *testing.T) {
	outdir := t.TempDir()

	// Start a (writable) server
	addr, cancel := startChunkServer(t, "-s", "testdata/blob1.store", "--key", "testdata/server.key", "--cert", "testdata/server.crt")
	defer cancel()
	_, port, _ := net.SplitHostPort(addr)
	store := fmt.Sprintf("https://localhost:%s/", port)

	// Run the "extract" command to confirm the TLS chunk server can be used
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"--ca-cert", "testdata/ca.crt", "-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	extractCmd.SetOutput(io.Discard)
	_, err := extractCmd.ExecuteC()
	require.NoError(t, err)
}

func TestChunkServerInsecureTLS(t *testing.T) {
	outdir := t.TempDir()

	stderr = io.Discard
	stdout = io.Discard

	// Start a (writable) server
	addr, cancel := startChunkServer(t, "-s", "testdata/blob1.store", "--key", "testdata/server.key", "--cert", "testdata/server.crt")
	defer cancel()
	_, port, _ := net.SplitHostPort(addr)
	store := fmt.Sprintf("https://localhost:%s/", port)

	// Run the "extract" command accepting any cert  to confirm the TLS chunk server can be used
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"-t", "-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	// extractCmd.SetOutput(io.Discard)
	_, err := extractCmd.ExecuteC()
	require.NoError(t, err)

	// Run the "extract" command without accepting any cert. Should fail.
	extractCmd = newExtractCommand(context.Background())
	extractCmd.SetOutput(io.Discard)
	extractCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	extractCmd.SetOutput(io.Discard)
	_, err = extractCmd.ExecuteC()
	require.Error(t, err)

}

func TestChunkServerMutualTLS(t *testing.T) {
	outdir := t.TempDir()

	stderr = io.Discard
	stdout = io.Discard

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
	_, err := extractCmd.ExecuteC()
	require.NoError(t, err)

	// Same without client certs, should fail.
	extractCmd = newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{
		"--ca-cert", "testdata/ca.crt",
		"-s", store, "testdata/blob1.caibx", filepath.Join(outdir, "blob1")})
	extractCmd.SetOutput(io.Discard)
	_, err = extractCmd.ExecuteC()
	require.Error(t, err)
}

func startChunkServer(t *testing.T, args ...string) (string, context.CancelFunc) {
	addr := freeLocalAddr(t)
	return addr, startChunkServerOnAddr(t, addr, args...)
}

// freeLocalAddr finds a free local address with a port that can be used to
// run a server on.
func freeLocalAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()
	return addr
}

// startChunkServerOnAddr starts a chunk server on the given address. The
// server goroutine reads package globals like the config during startup,
// so tests that modify those need to do that before calling this.
func startChunkServerOnAddr(t *testing.T, addr string, args ...string) context.CancelFunc {
	// Flush any handlers that were registered in the default mux before
	http.DefaultServeMux = &http.ServeMux{}

	// Start the server in a goroutine. Cancel the context when done
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newChunkServerCommand(ctx)
	cmd.SetArgs(append(args, "-l", addr))
	go func() {
		if _, err := cmd.ExecuteC(); err != nil && ctx.Err() == nil {
			t.Errorf("chunk server error: %v", err)
		}
	}()

	// Wait a little for the server to start
	time.Sleep(time.Second)
	return cancel
}

// randomEncryptionKey returns a new hex-encoded 256-bit chunk encryption key.
func randomEncryptionKey(t *testing.T) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return hex.EncodeToString(key)
}

func TestChunkServerEncryption(t *testing.T) {
	outdir := t.TempDir()
	testEncryptionKey := randomEncryptionKey(t)
	addr := freeLocalAddr(t)
	store := fmt.Sprintf("http://%s/", addr)

	// Build a client config. The client needs to be setup to talk to the HTTP chunk server
	// compressed+encrypted. Create a temp JSON config for that HTTP store and load it.
	// This has to happen before the server starts since its goroutine reads the same
	// config globals. Restore them afterwards so no other test picks up these options.
	oldCfgFile, oldCfg := cfgFile, cfg
	t.Cleanup(func() { cfgFile, cfg = oldCfgFile, oldCfg })
	cfgFile = filepath.Join(outdir, "config.json")
	cfg = Config{}
	cfgFileContent := fmt.Sprintf(`{"store-options": {"%s":{"encryption": true, "encryption-key": "%s"}}}`, store, testEncryptionKey)
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfgFileContent), 0644))
	initConfig()

	// Start a (writable) server, it'll expect compressed+encrypted chunks over
	// the wire while storing them only compressed in the local store
	cancel := startChunkServerOnAddr(t, addr, "-s", outdir, "-w", "--skip-verify-read=false", "--skip-verify-write=false", "--encryption-key", testEncryptionKey)
	defer cancel()

	// Run a "chop" command to send some chunks (encrypted) over HTTP, then have the server
	// store them un-encrypted in its local store.
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	chopCmd.SetOutput(io.Discard)
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// Now read it all back over HTTP (again encrypted) and re-assemble the test file
	extractFile := filepath.Join(outdir, "blob1")
	extractCmd := newExtractCommand(context.Background())
	extractCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", extractFile})
	extractCmd.SetOutput(io.Discard)
	_, err = extractCmd.ExecuteC()
	require.NoError(t, err)

	// Not actually necessary, but for good measure let's compare the blobs
	blobIn, err := os.ReadFile("testdata/blob1")
	require.NoError(t, err)
	blobOut, err := os.ReadFile(extractFile)
	require.NoError(t, err)
	require.Equal(t, blobIn, blobOut)
}

func TestChunkServerEnvKeyDoesNotEnableEncryption(t *testing.T) {
	// Having the key in the environment alone must not switch the server to
	// encrypted serving, it may be set for the sake of an encrypted store
	// elsewhere in the config
	t.Setenv("DESYNC_ENCRYPTION_KEY", randomEncryptionKey(t))

	// Reset any config a previous test may have loaded
	oldCfg := cfg
	cfg = Config{}
	t.Cleanup(func() { cfg = oldCfg })

	outdir := t.TempDir()
	addr, cancel := startChunkServer(t, "-s", outdir, "-w")
	defer cancel()
	store := fmt.Sprintf("http://%s/", addr)

	// A plain (compressed, unencrypted) client must be able to write chunks
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	chopCmd.SetOutput(io.Discard)
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// And the chunks have to be stored with the plain compressed extension
	matches, err := filepath.Glob(filepath.Join(outdir, "*", "*"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
	for _, m := range matches {
		require.True(t, strings.HasSuffix(m, ".cacnk"), "chunk %s is not a plain compressed chunk", m)
	}
}

func TestChunkServerEncryptionMissingKey(t *testing.T) {
	// Encryption enabled without a key from flag or environment has to be
	// rejected at startup rather than silently serving plaintext
	t.Setenv("DESYNC_ENCRYPTION_KEY", "")

	for _, args := range [][]string{
		{"--encryption"},
		{"--encryption-algorithm", "aes-256-gcm"},
	} {
		cmd := newChunkServerCommand(context.Background())
		cmd.SetArgs(append(args, "-s", t.TempDir(), "-l", "127.0.0.1:0"))
		cmd.SetOutput(io.Discard)
		_, err := cmd.ExecuteC()
		require.ErrorContains(t, err, "no encryption key configured")
	}
}
