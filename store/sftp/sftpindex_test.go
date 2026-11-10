package sftp

import (
	"io/fs"
	"net/url"
	"os"
	"runtime"
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/require"
)

// Indexes are always stored in plain form, encryption options need to be
// rejected rather than silently ignored.
func TestIndexStoreRejectsEncryption(t *testing.T) {
	u, err := url.Parse("sftp://host/store")
	require.NoError(t, err)
	_, err = NewIndexStore(u, desync.StoreOptions{Encryption: true})
	require.ErrorContains(t, err, "not supported by index stores")
}

func TestIndexStoreMissingIndex(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the test sftp server serves the local filesystem, which the store cannot address on windows")
	}
	t.Setenv("CASYNC_SSH_PATH", os.Args[0])
	t.Setenv("DESYNC_SFTP_TEST_SERVER", "1")

	u, err := url.Parse("sftp://localhost" + t.TempDir())
	require.NoError(t, err)
	s, err := NewIndexStore(u, desync.StoreOptions{})
	require.NoError(t, err)
	defer s.Close()

	f, err := os.Open("../../testdata/index.caibx")
	require.NoError(t, err)
	defer f.Close()
	idx, err := desync.IndexFromReader(f)
	require.NoError(t, err)

	// An index that is there can be read
	require.NoError(t, s.StoreIndex("index.caibx", idx))
	fromStore, err := s.GetIndex("index.caibx")
	require.NoError(t, err)
	require.Equal(t, idx, fromStore)

	// One that isn't is a missing object
	_, err = s.GetIndex("does-not-exist.caibx")
	require.ErrorIs(t, err, desync.NoSuchObject{Location: "does-not-exist.caibx"})
	require.ErrorIs(t, err, fs.ErrNotExist)
}
