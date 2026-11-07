package s3

import (
	"net/url"
	"testing"

	"github.com/folbricht/desync"
	minio "github.com/minio/minio-go/v7"
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
