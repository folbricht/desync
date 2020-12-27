package desync

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
)

// aes256ctr is an encryption layer for chunk storage. It
// encrypts/decrypts to/from storage using aes-256-ctr.
// The key is generated from a passphrase with SHA256.
type aes256ctr struct {
	key   []byte
	block cipher.Block
}

var _ converter = aes256ctr{}

func NewAES256CTR(passphrase string) (aes256ctr, error) {
	key := sha256.Sum256([]byte(passphrase))
	block, err := aes.NewCipher(key[:])
	return aes256ctr{key: key[:], block: block}, err
}

// encrypt for storage. The IV is prepended to the data.
func (d aes256ctr) toStorage(in []byte) ([]byte, error) {
	out := make([]byte, aes.BlockSize+len(in))
	iv := out[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(d.block, iv)
	stream.XORKeyStream(out[aes.BlockSize:], in)
	return out, nil
}

// decrypt from storage. The IV is taken from the start of the
// chunk data. This by itself does not verify integrity. That
// is achieved by the existing chunk validation.
func (d aes256ctr) fromStorage(in []byte) ([]byte, error) {
	if len(in) < aes.BlockSize {
		return nil, errors.New("no iv prefix found in chunk, not encrypted or wrong algorithm")
	}
	out := make([]byte, len(in)-aes.BlockSize)
	iv := in[:aes.BlockSize]
	stream := cipher.NewCTR(d.block, iv)
	stream.XORKeyStream(out, in[aes.BlockSize:])
	return out, nil
}

func (d aes256ctr) equal(c converter) bool {
	other, ok := c.(aes256ctr)
	if !ok {
		return false
	}
	return bytes.Equal(d.key, other.key)
}
