package desync

import (
	gnutar "archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.Nil(t, xattrsToPAXRecords(nil), "expected nil for nil xattrs")
	require.Nil(t, xattrsToPAXRecords(Xattrs{}), "expected nil for empty xattrs")

	// Keys must get the SCHILY.xattr. prefix archive/tar uses internally.
	in := Xattrs{"user.comment": "hello", "security.selinux": "ctx"}
	rec := xattrsToPAXRecords(in)
	want := map[string]string{
		"SCHILY.xattr.user.comment":     "hello",
		"SCHILY.xattr.security.selinux": "ctx",
	}
	require.Equal(t, want, rec)

	// Reverse must strip the prefix and ignore unrelated PAX records.
	rec["mtime"] = "12345"
	rec["path"] = "some/file"
	require.Equal(t, map[string]string(in), paxRecordsToXattrs(rec))
	require.Nil(t, paxRecordsToXattrs(map[string]string{"mtime": "1"}),
		"expected nil when no SCHILY records present")
	require.Nil(t, paxRecordsToXattrs(nil), "expected nil for nil records")
}

func TestTarXattrsRoundTrip(t *testing.T) {
	xattrs := Xattrs{"user.comment": "hello", "user.tag": "v1"}

	// CreateDir/File/Symlink hardcode FormatGNU, which archive/tar rejects
	// in combination with xattrs. CreateDevice leaves the format
	// unspecified, so the writer negotiates PAX and the xattr path is
	// actually exercised.
	var buf bytes.Buffer
	w := NewTarWriter(&buf)
	require.NoError(t, w.CreateDevice(NodeDevice{
		Name: "dev0", Mode: 0644, Major: 1, Minor: 3, Xattrs: xattrs,
	}))
	require.NoError(t, w.CreateDevice(NodeDevice{
		Name: "dev1", Mode: 0644, Major: 1, Minor: 4,
	}))
	require.NoError(t, w.Close())

	// On the wire the xattrs must be SCHILY.xattr.<name> PAX records,
	// exactly as the deprecated Header.Xattrs field produced them.
	sr := gnutar.NewReader(bytes.NewReader(buf.Bytes()))
	h, err := sr.Next()
	require.NoError(t, err)
	for k, v := range xattrs {
		require.Equal(t, v, h.PAXRecords["SCHILY.xattr."+k],
			"PAX record SCHILY.xattr.%s", k)
	}

	// Reading back through TarReader must reconstruct the xattr map, and a
	// node without xattrs must yield a nil map (unchanged behavior).
	rd := NewTarReader(bytes.NewReader(buf.Bytes()), TarReaderOptions{})
	f0, err := rd.Next()
	require.NoError(t, err)
	require.Equal(t, map[string]string(xattrs), f0.Xattrs)
	f1, err := rd.Next()
	require.NoError(t, err)
	require.Nil(t, f1.Xattrs, "expected nil xattrs for node without any")
	_, err = rd.Next()
	require.ErrorIs(t, err, io.EOF)
}
