package desync

import (
	"testing"
)

// Confirm that decompressing a blank chunk fails with an error and doesn't panic
func TestUncompressBlank(t *testing.T) {
	if _, err := Decompress(nil, nil); err == nil {
		t.Fatal("expected failure decompressing nil array")
	}
}
