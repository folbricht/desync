package desync

import "github.com/dchest/siphash"

// SipHash is used to calculate the hash in Goodbye element items, hashing the
// filename.
func SipHash(b []byte) uint64 {
	return siphash.Hash(CaFormatGoodbyeHashKey0, CaFormatGoodbyeHashKey1, b)
}
