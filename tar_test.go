// +build !windows

package desync

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTar(t *testing.T) {
	// First make a tempdir and create a few dirs and files in it
	base, err := ioutil.TempDir("", "desync-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)

	dirs := []string{
		"dir1/sub11",
		"dir1/sub12",
		"dir2/sub21",
		"dir2/sub22",
	}
	for _, d := range dirs {
		if err = os.MkdirAll(filepath.Join(base, d), 0755); err != nil {
			t.Fatal()
		}
	}

	files := []string{
		"dir1/sub11/f11",
		"dir1/sub11/f12",
	}
	for i, name := range files {
		ioutil.WriteFile(filepath.Join(base, name), []byte(fmt.Sprintf("filecontent%d", i)), 0644)
	}

	if err = os.Symlink("dir1", filepath.Join(base, "symlink")); err != nil {
		t.Fatal(err)
	}

	// Encode it all into a buffer
	fs := NewLocalFS(base, LocalFSOptions{})
	b := new(bytes.Buffer)
	if err = Tar(context.Background(), b, fs); err != nil {
		t.Fatal(err)
	}

	// Decode it again
	d := NewFormatDecoder(b)

	// Define an array of what is expected in the test file
	expected := []interface{}{
		FormatEntry{},
		FormatFilename{}, // "dir1"
		FormatEntry{},
		FormatFilename{}, // "sub11"
		FormatEntry{},
		FormatFilename{}, // "f11"
		FormatEntry{},
		FormatPayload{},
		FormatFilename{}, // "f12"
		FormatEntry{},
		FormatPayload{},
		FormatGoodbye{},
		FormatFilename{}, // "sub12"
		FormatEntry{},
		FormatGoodbye{},
		FormatGoodbye{},
		FormatFilename{}, // "dir2"
		FormatEntry{},
		FormatFilename{}, // "sub21"
		FormatEntry{},
		FormatGoodbye{},
		FormatFilename{}, // "sub22"
		FormatEntry{},
		FormatGoodbye{},
		FormatGoodbye{},
		FormatFilename{}, // "symlink"
		FormatEntry{},
		FormatSymlink{},
		FormatGoodbye{},
		nil,
	}

	for _, exp := range expected {
		v, err := d.Next()
		if err != nil {
			t.Fatal(err)
		}
		if reflect.TypeOf(exp) != reflect.TypeOf(v) {
			t.Fatalf("expected %s, got %s", reflect.TypeOf(exp), reflect.TypeOf(v))
		}
	}
}
