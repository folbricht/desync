package desync

import (
	"errors"
	"testing"
)

func TestNewChunkFromStorage(t *testing.T) {
	conv := Converters{Compressor{}}
	plain := []byte("the quick brown fox jumps over the lazy dog")
	id := Digest.Sum(plain)
	storage, err := conv.toStorage(plain)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("valid", func(t *testing.T) {
		c, err := NewChunkFromStorage(id, storage, conv, false)
		if err != nil {
			t.Fatal(err)
		}
		if c.ID() != id {
			t.Fatalf("got id %s, want %s", c.ID(), id)
		}
	})

	t.Run("undecodable storage data", func(t *testing.T) {
		// Truncated/garbage storage data fails to decompress. This must surface as
		// ChunkInvalid (so RepairableCache and 'verify --repair' still handle it) but
		// must also carry the underlying decode error rather than reporting a bogus
		// "does not match its hash 0000..." mismatch.
		_, err := NewChunkFromStorage(id, []byte("not a valid zstd stream"), conv, false)
		var ci ChunkInvalid
		if !errors.As(err, &ci) {
			t.Fatalf("expected ChunkInvalid, got %T: %v", err, err)
		}
		if ci.Err == nil {
			t.Fatal("expected ChunkInvalid.Err to carry the underlying decode error")
		}
		if errors.Unwrap(err) == nil {
			t.Fatal("expected the error to unwrap to the underlying decode error")
		}
	})

	t.Run("hash mismatch", func(t *testing.T) {
		var wrongID ChunkID
		wrongID[0] = 0x01
		_, err := NewChunkFromStorage(wrongID, storage, conv, false)
		var ci ChunkInvalid
		if !errors.As(err, &ci) {
			t.Fatalf("expected ChunkInvalid, got %T: %v", err, err)
		}
		if ci.Err != nil {
			t.Fatalf("expected ChunkInvalid.Err to be nil for a plain hash mismatch, got %v", ci.Err)
		}
		if ci.Sum != id {
			t.Fatalf("got sum %s, want %s", ci.Sum, id)
		}
	})

	t.Run("skip verify", func(t *testing.T) {
		// With skipVerify even undecodable data is accepted (no verification done).
		if _, err := NewChunkFromStorage(id, []byte("garbage"), conv, true); err != nil {
			t.Fatal(err)
		}
	})
}
