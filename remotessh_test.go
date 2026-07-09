package desync

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteSSHStoreRejectsEncryption(t *testing.T) {
	u, err := url.Parse("ssh://host/store")
	require.NoError(t, err)

	// The casync protocol doesn't support encryption, configuring it on an
	// ssh store needs to fail rather than being silently ignored
	_, err = NewRemoteSSHStore(u, StoreOptions{Encryption: true, EncryptionKey: testEncryptionKey})
	require.ErrorContains(t, err, "not supported")
}
