package desync

import (
	"bytes"
	"io/ioutil"
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

// Decode and then encode index/archive files to test the encode produces the
// exact same output.
func TestEncoder(t *testing.T) {
	files := []string{
		"testdata/index.caibx",
		"testdata/nested.catar",
	}
	for _, name := range files {
		in, err := ioutil.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}

		// Decoder
		d := NewFormatDecoder(bytes.NewReader(in))

		// Encoder
		out := new(bytes.Buffer)
		e := NewFormatEncoder(out)

		// Decode each element, then encode it again
		var total int64
		for {
			v, err := d.Next()
			if err != nil {
				t.Fatal(err)
			}
			if v == nil {
				break
			}
			n, err := e.Encode(v)
			if err != nil {
				t.Fatal(err)
			}
			total += n
		}

		// in/out should match
		if !bytes.Equal(in, out.Bytes()) {
			t.Fatalf("decoded/encoded don't match for file '%s'", name)
		}
		if total != int64(out.Len()) {
			t.Fatalf("unexpected length for encoding of '%s'", name)
		}
	}
}

// Goodbye items in a catar are a complete BST in array form. Test the sorting algorithm
// for those. The key in the BST is the hash.
func TestGoodbyeBST(t *testing.T) {
	in := []FormatGoodbyeItem{
		{Offset: 0x0, Hash: 0xb4bedf9e7796b4d},
		{Offset: 0x1, Hash: 0x218f89516a601c9c},
		{Offset: 0x2, Hash: 0x28b19de616c15f21},
		{Offset: 0x3, Hash: 0x490c091d8b45918f},
		{Offset: 0x4, Hash: 0x51ba5a19e058c7ad},
		{Offset: 0x5, Hash: 0x61cffdbff93ec8e0},
		{Offset: 0x6, Hash: 0x6b38ee3f1236bc32},
		{Offset: 0x7, Hash: 0x6ec111ca376a466e},
		{Offset: 0x8, Hash: 0x7d411df513f323cf},
		{Offset: 0x9, Hash: 0x9007695395e7df8f},
		{Offset: 0xa, Hash: 0x99a552eadd2d1199},
		{Offset: 0xb, Hash: 0x9e09fb7343978b70},
		{Offset: 0xc, Hash: 0xa1a7aeca9969d80a},
		{Offset: 0xd, Hash: 0xbcbe4464f8e3043b},
		{Offset: 0xe, Hash: 0xc01a4819ff41b89c},
		{Offset: 0xf, Hash: 0xc7bb588a3af1fb89},
	}

	expected := []FormatGoodbyeItem{
		{Offset: 0x8, Hash: 0x7d411df513f323cf},
		{Offset: 0x4, Hash: 0x51ba5a19e058c7ad},
		{Offset: 0xc, Hash: 0xa1a7aeca9969d80a},
		{Offset: 0x2, Hash: 0x28b19de616c15f21},
		{Offset: 0x6, Hash: 0x6b38ee3f1236bc32},
		{Offset: 0xa, Hash: 0x99a552eadd2d1199},
		{Offset: 0xe, Hash: 0xc01a4819ff41b89c},
		{Offset: 0x1, Hash: 0x218f89516a601c9c},
		{Offset: 0x3, Hash: 0x490c091d8b45918f},
		{Offset: 0x5, Hash: 0x61cffdbff93ec8e0},
		{Offset: 0x7, Hash: 0x6ec111ca376a466e},
		{Offset: 0x9, Hash: 0x9007695395e7df8f},
		{Offset: 0xb, Hash: 0x9e09fb7343978b70},
		{Offset: 0xd, Hash: 0xbcbe4464f8e3043b},
		{Offset: 0xf, Hash: 0xc7bb588a3af1fb89},
		{Offset: 0x0, Hash: 0xb4bedf9e7796b4d},
	}

	out := makeGoodbyeBST(in)

	if !reflect.DeepEqual(out, expected) {
		t.Fatal("BST doesn't match expected")
	}
}
