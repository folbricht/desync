package desync

import (
	"os"
	"reflect"
	"testing"
)

func TestFormatDecoder(t *testing.T) {
	f, err := os.Open("testdata/flat.catar")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	d := NewFormatDecoder(f)

	// Define an array of what is expected in the test file
	expected := []interface{}{
		FormatEntry{},
		FormatUser{},
		FormatGroup{},
		FormatSELinux{},
		FormatFilename{}, // "device"
		FormatEntry{},
		FormatSELinux{},
		FormatDevice{},
		FormatFilename{}, // "file1.txt"
		FormatEntry{},
		FormatUser{},
		FormatGroup{},
		FormatSELinux{},
		FormatPayload{},
		FormatFilename{}, // "file2.txt"
		FormatEntry{},
		FormatGroup{},
		FormatSELinux{},
		FormatPayload{},
		FormatFilename{}, // "symlink"
		FormatEntry{},
		FormatUser{},
		FormatGroup{},
		FormatSELinux{},
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

func TestIndexDecoder(t *testing.T) {
	f, err := os.Open("testdata/index.caibx")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	d := NewFormatDecoder(f)

	// The file should start with the index
	e, err := d.Next()
	if err != nil {
		t.Fatal(err)
	}
	index, ok := e.(FormatIndex)
	if !ok {
		t.Fatal("file doesn't start with an index")
	}
	if index.FeatureFlags != CaFormatSHA512256|CaFormatExcludeNoDump {
		t.Fatal("index flags don't match expected")
	}

	// Now get the table with the chunks
	e, err = d.Next()
	if err != nil {
		t.Fatal(err)
	}
	table, ok := e.(FormatTable)
	if !ok {
		t.Fatal("index table not found")
	}

	// Define the chunk IDs and the order they should be in the file
	expected := []string{
		"437884da2d1e61cf50b43b263ff15f25a870b0eae84bc22e4b5c307a0428764d",
		"985462e6b3293bbe61e43882686b481751ecf4b285bae4dffc2dfa8829f971ac",
		"fadff4b303624f2be3d0e04c2f105306118a9f608ef1e4f83c1babbd23a2315f",
	}
	// Check the expected length of the table
	if len(table.Items) != len(expected) {
		t.Fatalf("expected %d chunks in index table, got %d", len(expected), len(table.Items))
	}
	// And then make sure the IDs and order match
	for i := range expected {
		id, _ := ChunkIDFromString(expected[i])
		if table.Items[i].Chunk != id {
			t.Fatalf("expected chunk %s, got %s", id, table.Items[i].Chunk)
		}
	}
}
