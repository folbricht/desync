package desync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAES256CTREncryptDecrypt(t *testing.T) {
	plainIn := []byte{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	// Make two converters. One for encryption and one for decryption. Could use
	// just one but this way we confirm the key generation is consistent
	enc, err := NewAES256CTR("secret-password")
	require.NoError(t, err)
	dec, err := NewAES256CTR("secret-password")
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

	// Make another instance with a different password
	diffPw, err := NewAES256CTR("something-else")
	require.NoError(t, err)

	// Try to decrypt the data, should end up with garbage
	diffOut, err := diffPw.fromStorage(ciphertext)
	require.NoError(t, err)
	require.NotEqual(t, plainIn, diffOut)
}

func TestAES256CTRCompare(t *testing.T) {
	// Make three converters. Two with the same, one with a diff password
	enc1, err := NewAES256CTR("secret-password")
	require.NoError(t, err)
	enc2, err := NewAES256CTR("secret-password")
	require.NoError(t, err)
	diffPw, err := NewAES256CTR("something-else")
	require.NoError(t, err)

	// Check equality method
	require.True(t, enc1.equal(enc2))
	require.True(t, enc2.equal(enc1))
	require.False(t, diffPw.equal(enc1))
	require.False(t, enc1.equal(diffPw))
}
