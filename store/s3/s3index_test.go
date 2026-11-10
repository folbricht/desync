package s3

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/folbricht/desync"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// Indexes are always stored in plain form, encryption options need to be
// rejected rather than silently ignored.
func TestIndexStoreRejectsEncryption(t *testing.T) {
	u, err := url.Parse("s3+https://host/bucket")
	require.NoError(t, err)
	_, err = NewIndexStore(u, nil, "", desync.StoreOptions{Encryption: true}, minio.BucketLookupAuto)
	require.ErrorContains(t, err, "not supported by index stores")
}

// A missing index is only discovered when the object is read, not when the
// reader for it is set up. It needs to be reported as a missing object by
// both ways of getting an index.
func TestIndexStoreMissingIndex(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket/prefix/index.caibx" {
			http.ServeFile(w, r, "../../testdata/index.caibx")
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	u, err := url.Parse(ts.URL)
	require.NoError(t, err)

	location := url.URL{Scheme: "s3+http", Host: u.Host, Path: "/bucket/prefix/"}
	store, err := NewIndexStore(&location, credentials.New(&MockCredProvider{}), "region", desync.StoreOptions{}, minio.BucketLookupAuto)
	require.NoError(t, err)

	// An index that is there can be read with either
	idx, err := store.GetIndex("index.caibx")
	require.NoError(t, err)
	require.NotEmpty(t, idx.Chunks)

	r, err := store.GetIndexReader("index.caibx")
	require.NoError(t, err)
	fromReader, err := desync.IndexFromReader(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, idx, fromReader)

	// One that isn't is a missing object
	_, err = store.GetIndex("does-not-exist.caibx")
	require.ErrorIs(t, err, desync.NoSuchObject{Location: "does-not-exist.caibx"})
	require.ErrorIs(t, err, fs.ErrNotExist)

	_, err = store.GetIndexReader("does-not-exist.caibx")
	require.ErrorIs(t, err, desync.NoSuchObject{Location: "does-not-exist.caibx"})
}
