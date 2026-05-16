package desync

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestSafeComponent(t *testing.T) {
	valid := []string{"file", "dir1", "a.b", "..foo", "foo..", "name with space"}
	for _, n := range valid {
		if err := safeComponent(n); err != nil {
			t.Errorf("safeComponent(%q) = %v, want nil", n, err)
		}
	}
	invalid := []string{"", ".", "..", "evil/passwd", "a/b", "/abs", `a\b`, `\abs`}
	for _, n := range invalid {
		if err := safeComponent(n); err == nil {
			t.Errorf("safeComponent(%q) = nil, want error", n)
		}
	}
}

func TestConfined(t *testing.T) {
	in := []string{".", "a", "a/b", "a/../b", "./a"}
	for _, p := range in {
		if !confined(p) {
			t.Errorf("confined(%q) = false, want true", p)
		}
	}
	out := []string{"..", "../x", "a/../..", "/abs", "/"}
	for _, p := range out {
		if confined(p) {
			t.Errorf("confined(%q) = true, want false", p)
		}
	}
}

// TestArchiveDecoderRejectsEmbeddedSlash verifies the decoder rejects a
// FormatFilename whose name embeds a path separator (e.g. "evil/passwd"), the
// trick used to write through a previously-planted symlink. This protects
// every FilesystemWriter, including TarWriter which would otherwise forward
// the poisoned name into a produced tar.
func TestArchiveDecoderRejectsEmbeddedSlash(t *testing.T) {
	var buf bytes.Buffer
	enc := NewFormatEncoder(&buf)

	entry := FormatEntry{
		FormatHeader: FormatHeader{Size: 64, Type: CaFormatEntry},
		FeatureFlags: TarFeatureFlags,
		Mode:         os.ModeDir | 0755,
		MTime:        time.Unix(0, 0),
	}
	if _, err := enc.Encode(entry); err != nil {
		t.Fatal(err)
	}
	name := "evil/passwd"
	fn := FormatFilename{
		FormatHeader: FormatHeader{Size: uint64(16 + len(name) + 1), Type: CaFormatFilename},
		Name:         name,
	}
	if _, err := enc.Encode(fn); err != nil {
		t.Fatal(err)
	}

	d := NewArchiveDecoder(&buf)

	// First node is the (unnamed) root directory.
	v, err := d.Next()
	if err != nil {
		t.Fatalf("decoding root: %v", err)
	}
	if _, ok := v.(NodeDirectory); !ok {
		t.Fatalf("expected NodeDirectory, got %T", v)
	}

	// The embedded-slash filename must be rejected.
	_, err = d.Next()
	if err == nil {
		t.Fatal("expected error for embedded-slash filename, got nil")
	}
	if _, ok := err.(InvalidFormat); !ok {
		t.Fatalf("expected InvalidFormat, got %T: %v", err, err)
	}
}
