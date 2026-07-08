package desync

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// EncryptionKeySize is the size of the encryption key in bytes that is
// expected by the encryption converters. All supported algorithms use
// 256-bit keys.
const EncryptionKeySize = 32

// keyExtension builds the chunk file extension for an encryption
// algorithm. It contains a key ID, the leading 4 bytes of the SHA256
// hash of the key, to allow chunks encrypted with different keys to
// live in the same store without conflict.
func keyExtension(algorithm string, key []byte) string {
	keyHash := sha256.Sum256(key)
	return fmt.Sprintf(".%s-%x", algorithm, keyHash[:4])
}

// validateKey confirms the key is of the expected size for the
// supported 256-bit algorithms.
func validateKey(key []byte) error {
	if len(key) != EncryptionKeySize {
		return fmt.Errorf("encryption key must be %d bytes, got %d", EncryptionKeySize, len(key))
	}
	return nil
}

// sealWithRandomNonce encrypts data with a random nonce which is
// prepended to the output.
func sealWithRandomNonce(aead cipher.AEAD, in []byte) ([]byte, error) {
	out := make([]byte, aead.NonceSize(), aead.NonceSize()+len(in)+aead.Overhead())
	nonce := out[:aead.NonceSize()]
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(out, nonce, in, nil), nil
}

// openWithNoncePrefix decrypts data that has the nonce prepended.
func openWithNoncePrefix(aead cipher.AEAD, in []byte) ([]byte, error) {
	if len(in) < aead.NonceSize() {
		return nil, errors.New("no nonce prefix found in chunk, not encrypted or wrong algorithm")
	}
	nonce := in[:aead.NonceSize()]
	return aead.Open(nil, nonce, in[aead.NonceSize():], nil)
}

// xchacha20poly1305 is an encryption layer for chunk storage. It
// encrypts/decrypts to/from storage using XChaCha20-Poly1305 AEAD
// with a 256-bit key.
type xchacha20poly1305 struct {
	key  []byte
	aead cipher.AEAD

	// Chunk extension with identifier derived from the key.
	extension string
}

var _ converter = xchacha20poly1305{}

// NewXChaCha20Poly1305 returns a converter that encrypts chunks with
// XChaCha20-Poly1305 using the given 256-bit key.
func NewXChaCha20Poly1305(key []byte) (xchacha20poly1305, error) {
	if err := validateKey(key); err != nil {
		return xchacha20poly1305{}, err
	}
	aead, err := chacha20poly1305.NewX(key)
	return xchacha20poly1305{key: key, aead: aead, extension: keyExtension("xchacha20-poly1305", key)}, err
}

// encrypt for storage. The nonce is prepended to the data.
func (d xchacha20poly1305) toStorage(in []byte) ([]byte, error) {
	return sealWithRandomNonce(d.aead, in)
}

// decrypt from storage. The nonce is taken from the start of the
// chunk data.
func (d xchacha20poly1305) fromStorage(in []byte) ([]byte, error) {
	return openWithNoncePrefix(d.aead, in)
}

func (d xchacha20poly1305) equal(c converter) bool {
	other, ok := c.(xchacha20poly1305)
	if !ok {
		return false
	}
	return bytes.Equal(d.key, other.key)
}

func (d xchacha20poly1305) storageExtension() string {
	return d.extension
}

// aes256gcm is an encryption layer for chunk storage. It
// encrypts/decrypts to/from storage using AES-256-GCM with a
// 256-bit key.
type aes256gcm struct {
	key  []byte
	aead cipher.AEAD

	// Chunk extension with identifier derived from the key.
	extension string
}

var _ converter = aes256gcm{}

// NewAES256GCM returns a converter that encrypts chunks with
// AES-256-GCM using the given 256-bit key.
func NewAES256GCM(key []byte) (aes256gcm, error) {
	if err := validateKey(key); err != nil {
		return aes256gcm{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return aes256gcm{}, err
	}
	aead, err := cipher.NewGCM(block)
	return aes256gcm{key: key, aead: aead, extension: keyExtension("aes-256-gcm", key)}, err
}

// encrypt for storage. The nonce is prepended to the data.
func (d aes256gcm) toStorage(in []byte) ([]byte, error) {
	return sealWithRandomNonce(d.aead, in)
}

// decrypt from storage. The nonce is taken from the start of the
// chunk data.
func (d aes256gcm) fromStorage(in []byte) ([]byte, error) {
	return openWithNoncePrefix(d.aead, in)
}

func (d aes256gcm) equal(c converter) bool {
	other, ok := c.(aes256gcm)
	if !ok {
		return false
	}
	return bytes.Equal(d.key, other.key)
}

func (d aes256gcm) storageExtension() string {
	return d.extension
}
