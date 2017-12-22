package desync

import (
	"os"
	"reflect"
	"testing"
)

func TestArchiveDecoderTypes(t *testing.T) {
	f, err := os.Open("testdata/flat.catar")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	d := NewArchiveDecoder(f)

	// Define an array of what is expected in the test file
	expected := []interface{}{
		NodeDirectory{},
		NodeDevice{},
		NodeFile{},
		NodeFile{},
		NodeSymlink{},
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

func TestArchiveDecoderNesting(t *testing.T) {
	f, err := os.Open("testdata/nested.catar")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	d := NewArchiveDecoder(f)

	// Define an array of what is expected in the test file
	expected := []struct {
		Type interface{}
		Name string
		UID  int
		GID  int
	}{
		{Type: NodeDirectory{}, Name: ".", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir1", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir1/sub11", UID: 500, GID: 500},
		{Type: NodeFile{}, Name: "dir1/sub11/f11", UID: 500, GID: 500},
		{Type: NodeFile{}, Name: "dir1/sub11/f12", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir1/sub12", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir2", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir2/sub21", UID: 500, GID: 500},
		{Type: NodeDirectory{}, Name: "dir2/sub22", UID: 500, GID: 500},
		{Type: nil},
	}

	for _, e := range expected {
		v, err := d.Next()
		if err != nil {
			t.Fatal(err)
		}
		if reflect.TypeOf(e.Type) != reflect.TypeOf(v) {
			t.Fatalf("expected %s, got %s", reflect.TypeOf(e.Type), reflect.TypeOf(v))
		}
		if e.Type == nil {
			break
		}
		switch val := v.(type) {
		case NodeDirectory:
			if val.Name != e.Name {
				t.Fatalf("expected name '%s', got '%s'", e.Name, val.Name)
			}
			if val.UID != e.UID {
				t.Fatalf("expected uid '%d', got '%d'", e.UID, val.UID)
			}
		case NodeFile:
			if val.Name != e.Name {
				t.Fatalf("expected name '%s', got '%s'", e.Name, val.Name)
			}
			if val.UID != e.UID {
				t.Fatalf("expected uid '%d', got '%d'", e.UID, val.UID)
			}
		}
	}
}
