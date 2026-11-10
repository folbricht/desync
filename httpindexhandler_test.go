package desync

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingIndexStore is an index store that fails every read with a given
// error.
type failingIndexStore struct {
	err error
}

func (s failingIndexStore) GetIndexReader(string) (io.ReadCloser, error) { return nil, s.err }
func (s failingIndexStore) GetIndex(string) (Index, error)               { return Index{}, s.err }
func (s failingIndexStore) Close() error                                 { return nil }
func (s failingIndexStore) String() string                               { return "failing" }

func TestNoSuchObjectIsNotExist(t *testing.T) {
	err := NoSuchObject{Location: "index.caibx"}
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.ErrorIs(t, fmt.Errorf("store: %w", err), fs.ErrNotExist)
	require.NotErrorIs(t, err, fs.ErrPermission)
}

// TestHTTPIndexHandlerMissingIndex confirms the handler tells an index that
// isn't in the store from one that couldn't be read, regardless of how the
// store reports a missing index.
func TestHTTPIndexHandlerMissingIndex(t *testing.T) {
	_, notExist := os.Open(filepath.Join(t.TempDir(), "index.caibx"))
	require.Error(t, notExist)

	tests := map[string]struct {
		err    error
		status int
	}{
		"local file":          {notExist, http.StatusNotFound},
		"missing object":      {NoSuchObject{Location: "index.caibx"}, http.StatusNotFound},
		"wrapped not found":   {fmt.Errorf("store: %w", NoSuchObject{Location: "index.caibx"}), http.StatusNotFound},
		"failure to read":     {errors.New("connection refused"), http.StatusBadRequest},
		"wrapped read failed": {fmt.Errorf("store: %w", fs.ErrPermission), http.StatusBadRequest},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(NewHTTPIndexHandler(failingIndexStore{test.err}, false, ""))
			defer server.Close()

			for _, method := range []string{http.MethodGet, http.MethodHead} {
				req, err := http.NewRequest(method, server.URL+"/index.caibx", nil)
				require.NoError(t, err)
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				assert.Equal(t, test.status, resp.StatusCode, method)
			}
		})
	}
}

// TestHTTPIndexHandlerChained puts an index server in front of another one.
// An index missing from the store at the far end has to be reported as
// missing by both, and to the client as well.
func TestHTTPIndexHandlerChained(t *testing.T) {
	local, err := NewLocalIndexStore("testdata")
	require.NoError(t, err)
	upstream := httptest.NewServer(NewHTTPIndexHandler(local, false, ""))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	remote, err := NewRemoteHTTPIndexStore(upstreamURL, StoreOptions{})
	require.NoError(t, err)
	server := httptest.NewServer(NewHTTPIndexHandler(remote, false, ""))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := NewRemoteHTTPIndexStore(serverURL, StoreOptions{})
	require.NoError(t, err)

	_, err = client.GetIndex("index.caibx")
	require.NoError(t, err)

	_, err = client.GetIndex("does-not-exist.caibx")
	require.ErrorIs(t, err, fs.ErrNotExist)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req, err := http.NewRequest(method, server.URL+"/does-not-exist.caibx", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, method)
	}
}
