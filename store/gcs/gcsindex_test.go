package gcs

import (
	"net/url"
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/require"
)

// Indexes are always stored in plain form, encryption options need to be
// rejected rather than silently ignored.
func TestIndexStoreRejectsEncryption(t *testing.T) {
	u, err := url.Parse("gs://bucket/prefix")
	require.NoError(t, err)
	_, err = NewIndexStore(u, desync.StoreOptions{Encryption: true})
	require.ErrorContains(t, err, "not supported by index stores")
}
