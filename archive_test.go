package desync

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveDecoderTypes(t *testing.T) {
	f, err := os.Open("testdata/flat.catar")
	require.NoError(t, err)
	defer f.Close()

	d := NewArchiveDecoder(f)

	// Define an array of what is expected in the test file
	expected := []any{
		NodeDirectory{},
		NodeDevice{},
		NodeFile{},
		NodeFile{},
		NodeSymlink{},
		nil,
	}

	for _, exp := range expected {
		v, err := d.Next()
		require.NoError(t, err)
		require.IsType(t, exp, v)
	}
}

func TestArchiveDecoderNesting(t *testing.T) {
	f, err := os.Open("testdata/nested.catar")
	require.NoError(t, err)
	defer f.Close()

	d := NewArchiveDecoder(f)

	// Define an array of what is expected in the test file
	expected := []struct {
		Type any
		Name string
		UID  int
		GID  int
	}{
		{Type: NodeDirectory{}, Name: ".", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir1", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: path.Join("dir1", "sub11"), UID: 500, GID: 500},
		{Type: NodeFile{}, Name: path.Join("dir1", "sub11", "f11"), UID: 500, GID: 500},
		{Type: NodeFile{}, Name: path.Join("dir1", "sub11", "f12"), UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: path.Join("dir1", "sub12"), UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir2", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: path.Join("dir2", "sub21"), UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: path.Join("dir2", "sub22"), UID: 500, GID: 500},
		{Type: nil},
	}

	for _, e := range expected {
		v, err := d.Next()
		require.NoError(t, err)
		require.IsType(t, e.Type, v)
		if e.Type == nil {
			break
		}
		switch val := v.(type) {
		case NodeDirectory:
			require.Equal(t, e.Name, val.Name)
			require.Equal(t, e.UID, val.UID)
		case NodeFile:
			require.Equal(t, e.Name, val.Name)
			require.Equal(t, e.UID, val.UID)
		}
	}
}
