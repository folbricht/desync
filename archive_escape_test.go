package desync

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeComponent(t *testing.T) {
	valid := []string{"file", "dir1", "a.b", "..foo", "foo..", "name with space"}
	for _, n := range valid {
		assert.NoError(t, safeComponent(n), "safeComponent(%q)", n)
	}
	invalid := []string{"", ".", "..", "evil/passwd", "a/b", "/abs", `a\b`, `\abs`}
	for _, n := range invalid {
		assert.Error(t, safeComponent(n), "safeComponent(%q)", n)
	}
}

func TestConfined(t *testing.T) {
	in := []string{".", "a", "a/b", "a/../b", "./a"}
	for _, p := range in {
		assert.True(t, confined(p), "confined(%q)", p)
	}
	out := []string{"..", "../x", "a/../..", "/abs", "/"}
	for _, p := range out {
		assert.False(t, confined(p), "confined(%q)", p)
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
	_, err := enc.Encode(entry)
	require.NoError(t, err)
	name := "evil/passwd"
	fn := FormatFilename{
		FormatHeader: FormatHeader{Size: uint64(16 + len(name) + 1), Type: CaFormatFilename},
		Name:         name,
	}
	_, err = enc.Encode(fn)
	require.NoError(t, err)

	d := NewArchiveDecoder(&buf)

	// First node is the (unnamed) root directory.
	v, err := d.Next()
	require.NoError(t, err, "decoding root")
	require.IsType(t, NodeDirectory{}, v)

	// The embedded-slash filename must be rejected.
	_, err = d.Next()
	require.Error(t, err, "expected error for embedded-slash filename")
	require.IsType(t, InvalidFormat{}, err)
}
