package desync

import (
	"io"
	"os"
	"syscall"
	"time"
)

func isDevice(m os.FileMode) bool {
	return m&os.ModeDevice != 0
}

// FilesystemWriter is a filesystem implementation that supports untar'ing
// a catar archive to.
type FilesystemWriter interface {
	CreateDir(n NodeDirectory) error
	CreateFile(n NodeFile) error
	CreateSymlink(n NodeSymlink) error
	CreateDevice(n NodeDevice) error
}

// FilesystemReader is an interface for source filesystem to be used during
// tar operations. Next() is expected to return files and directories in a
// consistent and stable order and return io.EOF when no further files are available.
type FilesystemReader interface {
	Next() (*File, error)
}

// File represents a filesystem object such as directoy, file, symlink or device.
// It's used when creating archives from a source filesystem which can be a real
// OS filesystem, or another archive stream such as tar.
type File struct {
	Name string
	Path string
	Mode os.FileMode

	Size uint64

	// Link target for symlinks
	LinkTarget string

	// Modification time
	ModTime time.Time

	// User/group IDs
	Uid int
	Gid int

	// Major/Minor for character or block devices
	DevMajor uint64
	DevMinor uint64

	// Extented attributes
	Xattrs map[string]string

	// File content. Nil for non-regular files.
	Data io.ReadCloser
}

func (f *File) IsDir() bool {
	return f.Mode.IsDir()
}

func (f *File) IsRegular() bool {
	return f.Mode.IsRegular()
}

func (f *File) IsSymlink() bool {
	return f.Mode&os.ModeSymlink != 0
}

func (f *File) IsDevice() bool {
	return f.Mode&os.ModeDevice != 0
}

// Close closes the file data reader if any. It's safe to call
// for non-regular files as well.
func (f *File) Close() error {
	if f.Data != nil {
		return f.Data.Close()
	}
	return nil
}

// StatModeToFilemode converts syscall mode to Go's os.Filemode value.
func StatModeToFilemode(mode uint32) os.FileMode {
	fm := os.FileMode(mode & 0777)
	switch mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fm |= os.ModeDevice
	case syscall.S_IFCHR:
		fm |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fm |= os.ModeDir
	case syscall.S_IFIFO:
		fm |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fm |= os.ModeSymlink
	case syscall.S_IFSOCK:
		fm |= os.ModeSocket
	}
	if mode&syscall.S_ISGID != 0 {
		fm |= os.ModeSetgid
	}
	if mode&syscall.S_ISUID != 0 {
		fm |= os.ModeSetuid
	}
	if mode&syscall.S_ISVTX != 0 {
		fm |= os.ModeSticky
	}
	return fm
}

// FilemodeToStatMode converts Go's os.Filemode value into the syscall equivalent.
func FilemodeToStatMode(mode os.FileMode) uint32 {
	o := uint32(mode.Perm())
	switch m := mode & os.ModeType; m {
	case os.ModeDevice:
		o |= syscall.S_IFBLK
	case os.ModeDevice | os.ModeCharDevice:
		o |= syscall.S_IFCHR
	case os.ModeDir:
		o |= syscall.S_IFDIR
	case os.ModeNamedPipe:
		o |= syscall.S_IFIFO
	case os.ModeSymlink:
		o |= syscall.S_IFLNK
	case os.ModeSocket:
		o |= syscall.S_IFSOCK
	default:
		o |= syscall.S_IFREG
	}

	if mode&os.ModeSetuid != 0 {
		o |= syscall.S_ISUID
	}
	if mode&os.ModeSetgid != 0 {
		o |= syscall.S_ISGID
	}
	if mode&os.ModeSticky != 0 {
		o |= syscall.S_ISVTX
	}
	return o
}
