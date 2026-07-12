package desync

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stdioServerConn adapts the process's stdin/stdout to the ReadWriteCloser
// the sftp server operates on.
type stdioServerConn struct {
	io.Reader
	io.WriteCloser
}

// TestMain doubles as the fake ssh command for the SFTP store tests. The
// store shells out to ssh for the sftp subsystem, so tests point
// CASYNC_SSH_PATH at the test binary itself and set an environment variable
// that makes it serve the SFTP protocol on stdin/stdout instead of running
// the tests.
func TestMain(m *testing.M) {
	if os.Getenv("DESYNC_SFTP_TEST_SERVER") == "1" {
		server, err := sftp.NewServer(stdioServerConn{os.Stdin, os.Stdout})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := server.Serve(); err != nil && err != io.EOF {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSFTPStorePruneSingleConnection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the test sftp server serves the local filesystem, which the store cannot address on windows")
	}
	t.Setenv("CASYNC_SSH_PATH", os.Args[0])
	t.Setenv("DESYNC_SFTP_TEST_SERVER", "1")

	u, err := url.Parse("sftp://localhost" + t.TempDir())
	require.NoError(t, err)

	s, err := NewSFTPStore(u, StoreOptions{N: 1})
	require.NoError(t, err)
	defer s.Close()

	keep := NewChunk([]byte("chunk to keep"))
	prune := NewChunk([]byte("chunk to prune"))
	keepID := keep.ID()
	pruneID := prune.ID()
	require.NoError(t, s.StoreChunk(keep))
	require.NoError(t, s.StoreChunk(prune))

	// Prune with a pool of just one connection. This deadlocked when the
	// removal of a chunk waited for a second connection while the prune
	// held the only one.
	done := make(chan error)
	go func() {
		done <- s.Prune(context.Background(), map[ChunkID]struct{}{keepID: {}})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("prune deadlocked with a single connection")
	}

	hasKeep, err := s.HasChunk(keepID)
	require.NoError(t, err)
	assert.True(t, hasKeep, "chunk in the keep list was removed")
	hasPrune, err := s.HasChunk(pruneID)
	require.NoError(t, err)
	assert.False(t, hasPrune, "unreferenced chunk was not removed")
}
