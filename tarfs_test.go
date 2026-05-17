package desync

import (
	gnutar "archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"reflect"
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
	exp, err := os.ReadFile("testdata/complex.gnu-tar")
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

func TestTarXattrHelpers(t *testing.T) {
	// Empty/nil input must yield a nil map so the header stays in its
	// original (non-PAX) format.
	if r := xattrsToPAXRecords(nil); r != nil {
		t.Fatalf("expected nil for nil xattrs, got %v", r)
	}
	if r := xattrsToPAXRecords(Xattrs{}); r != nil {
		t.Fatalf("expected nil for empty xattrs, got %v", r)
	}

	// Keys must get the SCHILY.xattr. prefix archive/tar uses internally.
	in := Xattrs{"user.comment": "hello", "security.selinux": "ctx"}
	rec := xattrsToPAXRecords(in)
	want := map[string]string{
		"SCHILY.xattr.user.comment":     "hello",
		"SCHILY.xattr.security.selinux": "ctx",
	}
	if !reflect.DeepEqual(rec, want) {
		t.Fatalf("xattrsToPAXRecords = %v, want %v", rec, want)
	}

	// Reverse must strip the prefix and ignore unrelated PAX records.
	rec["mtime"] = "12345"
	rec["path"] = "some/file"
	if got := paxRecordsToXattrs(rec); !reflect.DeepEqual(got, map[string]string(in)) {
		t.Fatalf("paxRecordsToXattrs = %v, want %v", got, in)
	}
	if x := paxRecordsToXattrs(map[string]string{"mtime": "1"}); x != nil {
		t.Fatalf("expected nil when no SCHILY records present, got %v", x)
	}
	if x := paxRecordsToXattrs(nil); x != nil {
		t.Fatalf("expected nil for nil records, got %v", x)
	}
}

func TestTarXattrsRoundTrip(t *testing.T) {
	xattrs := Xattrs{"user.comment": "hello", "user.tag": "v1"}

	// CreateDir/File/Symlink hardcode FormatGNU, which archive/tar rejects
	// in combination with xattrs. CreateDevice leaves the format
	// unspecified, so the writer negotiates PAX and the xattr path is
	// actually exercised.
	var buf bytes.Buffer
	w := NewTarWriter(&buf)
	if err := w.CreateDevice(NodeDevice{
		Name: "dev0", Mode: 0644, Major: 1, Minor: 3, Xattrs: xattrs,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.CreateDevice(NodeDevice{
		Name: "dev1", Mode: 0644, Major: 1, Minor: 4,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// On the wire the xattrs must be SCHILY.xattr.<name> PAX records,
	// exactly as the deprecated Header.Xattrs field produced them.
	sr := gnutar.NewReader(bytes.NewReader(buf.Bytes()))
	h, err := sr.Next()
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range xattrs {
		if got := h.PAXRecords["SCHILY.xattr."+k]; got != v {
			t.Fatalf("PAX record SCHILY.xattr.%s = %q, want %q", k, got, v)
		}
	}

	// Reading back through TarReader must reconstruct the xattr map, and a
	// node without xattrs must yield a nil map (unchanged behavior).
	rd := NewTarReader(bytes.NewReader(buf.Bytes()), TarReaderOptions{})
	f0, err := rd.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f0.Xattrs, map[string]string(xattrs)) {
		t.Fatalf("round-tripped xattrs = %v, want %v", f0.Xattrs, xattrs)
	}
	f1, err := rd.Next()
	if err != nil {
		t.Fatal(err)
	}
	if f1.Xattrs != nil {
		t.Fatalf("expected nil xattrs for node without any, got %v", f1.Xattrs)
	}
	if _, err := rd.Next(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}
