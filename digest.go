package desync

import (
	"crypto"
	"crypto/sha256"
	"crypto/sha512"
)

// Digest algorithm used globally for all chunk hashing. Can be set to SHA512256
// (default) or to SHA256.
var Digest HashAlgorithm = SHA512256{}

// HashAlgorithm is a digest algorithm used to hash chunks.
type HashAlgorithm interface {
	Sum([]byte) [32]byte
	Algorithm() crypto.Hash
}

// SHA512-256 hashing algoritm for Digest.
type SHA512256 struct{}

func (h SHA512256) Sum(data []byte) [32]byte { return sha512.Sum512_256(data) }
func (h SHA512256) Algorithm() crypto.Hash   { return crypto.SHA512_256 }

// SHA256 hashing algoritm for Digest.
type SHA256 struct{}

func (h SHA256) Sum(data []byte) [32]byte { return sha256.Sum256(data) }
func (h SHA256) Algorithm() crypto.Hash   { return crypto.SHA256 }
