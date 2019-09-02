package desync

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"
)

func TestGnuTarWrite(t *testing.T) {
	// Input catar archive
	r, err := os.Open("testdata/complex.catar")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Expected output
	exp, err := ioutil.ReadFile("testdata/complex.gnu-tar")
	if err != nil {
		t.Fatal(err)
	}

	// Output GNU tar archive
	b := new(bytes.Buffer)

	// Write in GNU tar format
	fs := NewTarWriter(b)
	if err := UnTar(context.Background(), r, fs); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	// Compare to expected
	if !bytes.Equal(b.Bytes(), exp) {
		t.Fatal("tar file does not match expected")
	}
}
