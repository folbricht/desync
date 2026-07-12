package desync

import (
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/require"
)

// Hex-encoded 256-bit keys used in encryption tests.
const (
	testEncryptionKey  = "6368616e676520746869732070617373776f726420746f206120736563726574"
	otherEncryptionKey = "746f74616c6c7920646966666572656e74206b65792075736564206865726521"
)

func testKey(t *testing.T, s string) []byte {
	key, err := hex.DecodeString(s)
	require.NoError(t, err)
	return key
}

func TestEncryptDecrypt(t *testing.T) {
	tests := map[string]struct {
		alg func([]byte) (converter, error)
	}{
		"xchacha20-poly1305": {func(key []byte) (converter, error) { return NewXChaCha20Poly1305(key) }},
		"aes-256-gcm":        {func(key []byte) (converter, error) { return NewAES256GCM(key) }},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			plainIn := []byte{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

			// Make two converters. One for encryption and one for decryption. Could use
			// just one but this way we confirm the setup from a key is consistent
			enc, err := test.alg(testKey(t, testEncryptionKey))
			require.NoError(t, err)
			dec, err := test.alg(testKey(t, testEncryptionKey))
			require.NoError(t, err)

			// Encrypt the data
			ciphertext, err := enc.toStorage(plainIn)
			require.NoError(t, err)

			// Confirm the ciphertext is actually different than what went in
			require.NotEqual(t, plainIn, ciphertext)

			// Decrypt it
			plainOut, err := dec.fromStorage(ciphertext)
			require.NoError(t, err)

			// This should match the original data of course
			require.Equal(t, plainIn, plainOut)

			// Make another instance with a different key
			diffKey, err := test.alg(testKey(t, otherEncryptionKey))
			require.NoError(t, err)

			// Try to decrypt the data, should get an error from AEAD algorithms
			_, err = diffKey.fromStorage(ciphertext)
			require.Error(t, err)

			// Keys need to be 256-bit, anything else should be rejected
			_, err = test.alg([]byte("too short"))
			require.Error(t, err)
		})
	}
}

func TestAES256GCMCompare(t *testing.T) {
	// Make three converters. Two with the same, one with a diff key
	enc1, err := NewAES256GCM(testKey(t, testEncryptionKey))
	require.NoError(t, err)
	enc2, err := NewAES256GCM(testKey(t, testEncryptionKey))
	require.NoError(t, err)
	diffKey, err := NewAES256GCM(testKey(t, otherEncryptionKey))
	require.NoError(t, err)

	// Check equality method
	require.True(t, enc1.equal(enc2))
	require.True(t, enc2.equal(enc1))
	require.False(t, diffKey.equal(enc1))
	require.False(t, enc1.equal(diffKey))

	// A different algorithm with the same key must not be equal either
	diffAlg, err := NewXChaCha20Poly1305(testKey(t, testEncryptionKey))
	require.NoError(t, err)
	require.False(t, enc1.equal(diffAlg))
	require.False(t, diffAlg.equal(enc1))
}

func TestAES256GCMExtension(t *testing.T) {
	enc1, err := NewAES256GCM(testKey(t, testEncryptionKey))
	require.NoError(t, err)

	// Confirm that we have a key-handle in the file extension
	require.Equal(t, ".aes-256-gcm-50d64035", enc1.extension)

	// If algorithm and key are the same, the same key handle
	// (extension) should be produced every time
	enc2, err := NewAES256GCM(testKey(t, testEncryptionKey))
	require.NoError(t, err)
	require.Equal(t, enc1.extension, enc2.extension)
}

// Indexes are always stored in plain form, encryption options need to be
// rejected by index stores rather than silently ignored, for example when
// a config entry matches a location used for both chunks and indexes.
func TestIndexStoresRejectEncryption(t *testing.T) {
	opt := StoreOptions{Encryption: true, EncryptionKey: testEncryptionKey}

	tests := map[string]struct {
		location string
		newStore func(*url.URL) (IndexStore, error)
	}{
		"sftp": {"sftp://host/store", func(u *url.URL) (IndexStore, error) { return NewSFTPIndexStore(u, opt) }},
		"http": {"https://host/store", func(u *url.URL) (IndexStore, error) { return NewRemoteHTTPIndexStore(u, opt) }},
		"s3": {"s3+https://host/bucket", func(u *url.URL) (IndexStore, error) {
			return NewS3IndexStore(u, nil, "", opt, minio.BucketLookupAuto)
		}},
		"gs": {"gs://bucket/prefix", func(u *url.URL) (IndexStore, error) { return NewGCIndexStore(u, opt) }},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(test.location)
			require.NoError(t, err)
			_, err = test.newStore(u)
			require.ErrorContains(t, err, "not supported by index stores")
		})
	}
}

func TestStorageConvertersValidation(t *testing.T) {
	tests := map[string]struct {
		opt StoreOptions
		err string
	}{
		"key without encryption flag":       {StoreOptions{EncryptionKey: testEncryptionKey}, "without setting encryption to true"},
		"algorithm without encryption flag": {StoreOptions{EncryptionAlgorithm: "aes-256-gcm"}, "without setting encryption to true"},
		"encryption without key":            {StoreOptions{Encryption: true}, "no encryption key configured"},
		"invalid key encoding":              {StoreOptions{Encryption: true, EncryptionKey: "not-hex"}, "invalid encryption key"},
		"unsupported algorithm":             {StoreOptions{Encryption: true, EncryptionKey: testEncryptionKey, EncryptionAlgorithm: "rot13"}, "unsupported encryption algorithm"},
		"valid":                             {StoreOptions{Encryption: true, EncryptionKey: testEncryptionKey}, ""},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := test.opt.StorageConverters()
			if test.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.err)
			}
		})
	}
}
