package desync

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreallocateNewFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "new")

	require.NoError(t, preallocateFile(name, 2*1024*1024))

	b, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Len(t, b, 2*1024*1024)
	require.Equal(t, make([]byte, 2*1024*1024), b)
}

func TestPreallocateGrowExistingFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "grow")
	data := bytes.Repeat([]byte{0xab}, 4096)
	require.NoError(t, os.WriteFile(name, data, 0666))

	require.NoError(t, preallocateFile(name, 64*1024))

	b, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Len(t, b, 64*1024)
	// The original content must be preserved and the new region read as zeros
	require.Equal(t, data, b[:len(data)])
	require.Equal(t, make([]byte, 64*1024-len(data)), b[len(data):])
}

func TestPreallocateShrinkExistingFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "shrink")
	data := bytes.Repeat([]byte{0xab}, 64*1024)
	require.NoError(t, os.WriteFile(name, data, 0666))

	require.NoError(t, preallocateFile(name, 4096))

	b, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, data[:4096], b)
}

func TestPreallocateSameSize(t *testing.T) {
	name := filepath.Join(t.TempDir(), "same")
	data := bytes.Repeat([]byte{0xab}, 4096)
	require.NoError(t, os.WriteFile(name, data, 0666))

	require.NoError(t, preallocateFile(name, int64(len(data))))

	b, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, data, b)
}

func TestPreallocateEmptyFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "empty")

	require.NoError(t, preallocateFile(name, 0))

	info, err := os.Stat(name)
	require.NoError(t, err)
	require.Zero(t, info.Size())
}
