package desync

import (
	gnutar "archive/tar"
	"io"
	"io/ioutil"
	"path"
)

// TarWriter uses a GNU tar archive for tar/untar operations of a catar archive.
type TarWriter struct {
	w      *gnutar.Writer
	format gnutar.Format
}

var _ FilesystemWriter = TarWriter{}

// NewTarFS initializes a new instance of a GNU tar archive that can be used
// for catar archive tar/untar operations.
func NewTarWriter(w io.Writer) TarWriter {
	return TarWriter{gnutar.NewWriter(w), gnutar.FormatGNU}
}

func (fs TarWriter) CreateDir(n NodeDirectory) error {
	hdr := &gnutar.Header{
		Typeflag: gnutar.TypeDir,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Xattrs:   n.Xattrs,
		Format:   fs.format,
	}
	return fs.w.WriteHeader(hdr)
}

func (fs TarWriter) CreateFile(n NodeFile) error {
	hdr := &gnutar.Header{
		Typeflag: gnutar.TypeReg,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Size:     int64(n.Size),
		Xattrs:   n.Xattrs,
		Format:   fs.format,
	}
	if err := fs.w.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := io.Copy(fs.w, n.Data)
	return err
}

func (fs TarWriter) CreateSymlink(n NodeSymlink) error {
	hdr := &gnutar.Header{
		Typeflag: gnutar.TypeSymlink,
		Linkname: n.Target,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Xattrs:   n.Xattrs,
		Format:   fs.format,
	}
	return fs.w.WriteHeader(hdr)
}

// We're not using os.Filemode here but the low-level system modes where the mode bits
// are in the lower half. Can't use os.ModeCharDevice here.
const modeChar = 0x4000

func (fs TarWriter) CreateDevice(n NodeDevice) error {
	var typ byte = gnutar.TypeBlock
	if n.Mode&modeChar != 0 {
		typ = gnutar.TypeChar
	}
	hdr := &gnutar.Header{
		Typeflag: typ,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Xattrs:   n.Xattrs,
		Devmajor: int64(n.Major),
		Devminor: int64(n.Minor),
	}
	return fs.w.WriteHeader(hdr)
}

func (fs TarWriter) Close() error {
	return fs.w.Close()
}

// TarReader uses a GNU tar archive as source for a tar operation (to produce
// a catar).
type TarReader struct {
	r *gnutar.Reader
}

var _ FilesystemReader = TarReader{}

// NewTarFS initializes a new instance of a GNU tar archive that can be used
// for catar archive tar/untar operations.
func NewTarReader(r io.Reader) TarReader {
	return TarReader{gnutar.NewReader(r)}
}

// Next returns the next filesystem entry or io.EOF when done. The caller is responsible
// for closing the returned File object.
func (fs TarReader) Next() (*File, error) {
	h, err := fs.r.Next()
	if err != nil {
		return nil, err
	}

	info := h.FileInfo()

	f := &File{
		Name:       info.Name(),
		Path:       path.Clean(h.Name),
		Mode:       info.Mode(),
		ModTime:    info.ModTime(),
		Size:       uint64(info.Size()),
		LinkTarget: h.Linkname,
		Uid:        h.Uid,
		Gid:        h.Gid,
		Xattrs:     h.Xattrs,
		DevMajor:   uint64(h.Devmajor),
		DevMinor:   uint64(h.Devminor),
		Data:       ioutil.NopCloser(fs.r),
	}

	return f, nil
}
