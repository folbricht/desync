package desync

import (
	"crypto"
	"fmt"
	"io"
	"strings"
)

// MtreeFS prints the filesystem operations to a writer (which can be os.Stdout)
// in mtree format.
type MtreeFS struct {
	w io.Writer
}

var _ FilesystemWriter = MtreeFS{}

// NewLocalFS initializes a new instance of a local filesystem that
// can be used for tar/untar operations.
func NewMtreeFS(w io.Writer) (MtreeFS, error) {
	_, err := fmt.Fprintln(w, "#mtree v1.0")
	return MtreeFS{w: w}, err
}

func (fs MtreeFS) CreateDir(n NodeDirectory) error {
	attr := []string{mtreeFilename(n.Name), "type=dir"}
	attr = append(attr, fmt.Sprintf("mode=%04o", n.Mode.Perm()))
	attr = append(attr, fmt.Sprintf("uid=%d", n.UID))
	attr = append(attr, fmt.Sprintf("gid=%d", n.GID))
	attr = append(attr, fmt.Sprintf("time=%d.%9d", n.MTime.Unix(), n.MTime.Nanosecond()))
	fmt.Fprintln(fs.w, strings.Join(attr, " "))
	return nil
}

func (fs MtreeFS) CreateFile(n NodeFile) error {
	attr := []string{mtreeFilename(n.Name), "type=file"}
	attr = append(attr, fmt.Sprintf("mode=%04o", n.Mode.Perm()))
	attr = append(attr, fmt.Sprintf("uid=%d", n.UID))
	attr = append(attr, fmt.Sprintf("gid=%d", n.GID))
	attr = append(attr, fmt.Sprintf("size=%d", n.Size))
	attr = append(attr, fmt.Sprintf("time=%d.%09d", n.MTime.Unix(), n.MTime.Nanosecond()))

	switch Digest.Algorithm() {
	case crypto.SHA512_256:
		h := Digest.Algorithm().New()
		if _, err := io.Copy(h, n.Data); err != nil {
			return err
		}
		attr = append(attr, fmt.Sprintf("sha512256digest=%x", h.Sum(nil)))
	case crypto.SHA256:
		h := Digest.Algorithm().New()
		if _, err := io.Copy(h, n.Data); err != nil {
			return err
		}
		attr = append(attr, fmt.Sprintf("sha56digest=%x", h.Sum(nil)))
	default:
		return fmt.Errorf("unsupported mtree hash algorithm %d", Digest.Algorithm())
	}
	fmt.Fprintln(fs.w, strings.Join(attr, " "))
	return nil
}

func (fs MtreeFS) CreateSymlink(n NodeSymlink) error {
	attr := []string{mtreeFilename(n.Name), "type=link"}
	attr = append(attr, fmt.Sprintf("mode=%04o", n.Mode.Perm()))
	attr = append(attr, fmt.Sprintf("target=%s", mtreeFilename(n.Target)))
	attr = append(attr, fmt.Sprintf("uid=%d", n.UID))
	attr = append(attr, fmt.Sprintf("gid=%d", n.GID))
	attr = append(attr, fmt.Sprintf("time=%d.%9d", n.MTime.Unix(), n.MTime.Nanosecond()))
	fmt.Fprintln(fs.w, strings.Join(attr, " "))
	return nil
}

func (fs MtreeFS) CreateDevice(n NodeDevice) error {
	attr := []string{mtreeFilename(n.Name)}
	if n.Mode&modeChar != 0 {
		attr = append(attr, "type=char")
	} else {
		attr = append(attr, "type=block")
	}
	attr = append(attr, fmt.Sprintf("mode=%04o", n.Mode.Perm()))
	attr = append(attr, fmt.Sprintf("uid=%d", n.UID))
	attr = append(attr, fmt.Sprintf("gid=%d", n.GID))
	attr = append(attr, fmt.Sprintf("time=%d.%9d", n.MTime.Unix(), n.MTime.Nanosecond()))
	fmt.Fprintln(fs.w, strings.Join(attr, " "))
	return nil
}

// Converts filenames into an mtree-compatible format following the rules outined in mtree(5):
//
// When encoding file or pathnames, any backslash character or character outside of the 95
// printable ASCII characters must be encoded as a backslash followed by three octal digits.
// When reading mtree files, any appearance of a backslash followed by three octal digits should
// be converted into the corresponding character.
func mtreeFilename(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch {
		case c == '\\' || c == '#' || c < 32 || c > 126:
			b.WriteString(fmt.Sprintf("\\%03o", c))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
