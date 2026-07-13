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

// aeadConverter is an encryption layer for chunk storage. It
// encrypts/decrypts to/from storage with an AEAD algorithm using
// a 256-bit key. A random nonce is generated for every chunk and
// prepended to the ciphertext.
type aeadConverter struct {
	algorithm string
	key       []byte
	aead      cipher.AEAD

	// Chunk extension with identifier derived from the key.
	extension string
}

var _ converter = aeadConverter{}

// NewXChaCha20Poly1305 returns a converter that encrypts chunks with
// XChaCha20-Poly1305 using the given 256-bit key.
func NewXChaCha20Poly1305(key []byte) (aeadConverter, error) {
	if err := validateKey(key); err != nil {
		return aeadConverter{}, err
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return aeadConverter{}, err
	}
	return newAEADConverter("xchacha20-poly1305", key, aead), nil
}

// NewAES256GCM returns a converter that encrypts chunks with
// AES-256-GCM using the given 256-bit key.
func NewAES256GCM(key []byte) (aeadConverter, error) {
	if err := validateKey(key); err != nil {
		return aeadConverter{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return aeadConverter{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return aeadConverter{}, err
	}
	return newAEADConverter("aes-256-gcm", key, aead), nil
}

func newAEADConverter(algorithm string, key []byte, aead cipher.AEAD) aeadConverter {
	// The extension contains a key ID, the leading 4 bytes of the SHA256
	// hash of the key, to allow chunks encrypted with different keys to
	// live in the same store without conflict.
	keyHash := sha256.Sum256(key)
	extension := fmt.Sprintf(".%s-%x", algorithm, keyHash[:4])
	return aeadConverter{algorithm: algorithm, key: key, aead: aead, extension: extension}
}

// validateKey confirms the key is of the expected size for the
// supported 256-bit algorithms.
func validateKey(key []byte) error {
	if len(key) != EncryptionKeySize {
		return fmt.Errorf("encryption key must be %d bytes, got %d", EncryptionKeySize, len(key))
	}
	return nil
}

// encrypt for storage. The nonce is prepended to the data.
func (d aeadConverter) toStorage(in []byte) ([]byte, error) {
	out := make([]byte, d.aead.NonceSize(), d.aead.NonceSize()+len(in)+d.aead.Overhead())
	nonce := out[:d.aead.NonceSize()]
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return d.aead.Seal(out, nonce, in, nil), nil
}

// decrypt from storage. The nonce is taken from the start of the
// chunk data.
func (d aeadConverter) fromStorage(in []byte) ([]byte, error) {
	if len(in) < d.aead.NonceSize() {
		return nil, errors.New("no nonce prefix found in chunk, not encrypted or wrong algorithm")
	}
	nonce := in[:d.aead.NonceSize()]
	return d.aead.Open(nil, nonce, in[d.aead.NonceSize():], nil)
}

func (d aeadConverter) equal(c converter) bool {
	other, ok := c.(aeadConverter)
	if !ok {
		return false
	}
	return d.algorithm == other.algorithm && bytes.Equal(d.key, other.key)
}

func (d aeadConverter) storageExtension() string {
	return d.extension
}
