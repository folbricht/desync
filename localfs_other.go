//go:build !windows
// +build !windows

package desync

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pkg/xattr"
	"golang.org/x/sys/unix"
)

// NewLocalFS initializes a new instance of a local filesystem that
// can be used for tar/untar operations.
func NewLocalFS(root string, opts LocalFSOptions) *LocalFS {
	return &LocalFS{
		Root:    root,
		opts:    opts,
		entries: make(chan walkEntry),
	}
}

// setXattrs applies extended attributes to a regular file or directory using
// an fd opened through the root handle, so that no symlink in the path can be
// followed.
func setXattrs(r *os.Root, name string, xattrs Xattrs) error {
	if len(xattrs) == 0 {
		return nil
	}
	f, err := r.Open(name)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	defer f.Close()
	for key, value := range xattrs {
		if err := xattr.FSet(f, key, []byte(value)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// setXattrsNoFollow applies extended attributes to a symlink or device node.
// There is no fd-based equivalent for these, so a path under the resolved real
// root is used. All intermediate components were created by us through the
// root handle and are therefore confined to it.
func (fs *LocalFS) setXattrsNoFollow(name string, xattrs Xattrs) error {
	if len(xattrs) == 0 {
		return nil
	}
	dst := filepath.Join(fs.rootReal, name)
	for key, value := range xattrs {
		if err := xattr.LSet(dst, key, []byte(value)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// prepareDirWrite gives us the permissions needed to populate a directory,
// which the mode in the archive may not include. GNU tar does the same. The
// mode from the archive is restored by applyDirMetadata() once the directory
// is complete. Best-effort, if it fails the write that needs the permissions
// reports the real error.
func (fs *LocalFS) prepareDirWrite(r *os.Root, n NodeDirectory, existing os.FileMode, created bool) {
	if fs.opts.NoSamePermissions {
		// Nothing to restore later, so only relax what has to be relaxed.
		// Newly created directories use the umask default and are writable.
		if !created && existing&0300 != 0300 {
			_ = r.Chmod(n.Name, existing|0700)
		}
		return
	}
	// Newly created directories are chmod'ed as well, both to undo a umask
	// that stripped the bits we need and to set the setuid/setgid/sticky bits
	// that mkdir doesn't apply. A setgid directory has to carry that bit while
	// its contents are created for them to inherit the group.
	if created || existing&0300 != 0300 {
		_ = r.Chmod(n.Name, n.Mode|0700)
	}
}

func (fs *LocalFS) SetDirPermissions(n NodeDirectory) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	// The dir exists now, fix the UID/GID if needed
	if !fs.opts.NoSameOwner {
		if err := r.Chown(n.Name, n.UID, n.GID); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
		if err := setXattrs(r, n.Name, n.Xattrs); err != nil {
			return err
		}
	}
	if !fs.opts.NoSamePermissions {
		if err := r.Chmod(n.Name, n.Mode); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
	}

	return nil
}

func (fs *LocalFS) SetFilePermissions(n NodeFile) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	if !fs.opts.NoSameOwner {
		if err := r.Chown(n.Name, n.UID, n.GID); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
		if err := setXattrs(r, n.Name, n.Xattrs); err != nil {
			return err
		}
	}
	if !fs.opts.NoSamePermissions {
		if err := r.Chmod(n.Name, n.Mode); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
	}

	return nil
}

func (fs *LocalFS) SetSymlinkPermissions(n NodeSymlink) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	// TODO: On Linux, the permissions of the link don't matter so we don't
	// set them here. But they do matter somewhat on Mac, so should probably
	// add some Mac-specific logic for that here.
	// fchmodat() with flag AT_SYMLINK_NOFOLLOW
	if !fs.opts.NoSameOwner {
		if err := r.Lchown(n.Name, n.UID, n.GID); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
		if err := fs.setXattrsNoFollow(n.Name, n.Xattrs); err != nil {
			return err
		}
	}

	return nil
}

func (fs *LocalFS) CreateDevice(n NodeDevice) error {
	r, err := fs.writeRoot()
	if err != nil {
		return err
	}

	if err := fs.completeDirs(n.Name); err != nil {
		return err
	}

	if err := r.Remove(n.Name); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%s: %w", n.Name, err)
	}

	// os.Root has no Mknod. createDeviceNode is implemented per platform but
	// always confines the operation to the extraction root.
	if err := fs.createDeviceNode(r, n); err != nil {
		return fmt.Errorf("mknod %s: %w", n.Name, err)
	}

	if !fs.opts.NoSameOwner {
		if err := r.Chown(n.Name, n.UID, n.GID); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}
		if err := fs.setXattrsNoFollow(n.Name, n.Xattrs); err != nil {
			return err
		}
	}
	if !fs.opts.NoSamePermissions {
		if err := r.Chmod(n.Name, n.Mode); err != nil {
			return fmt.Errorf("chmod %s: %w", n.Name, err)
		}
	}
	if n.MTime.Equal(time.Unix(0, 0)) {
		return nil
	}
	return r.Chtimes(n.Name, n.MTime, n.MTime)
}

// mkdev encodes a major/minor pair from a catar into a device number. The
// layout of dev_t differs between operating systems, so this defers to
// x/sys/unix rather than hard-coding one platform's encoding.
func mkdev(major, minor uint64) (uint64, error) {
	if major > math.MaxUint32 || minor > math.MaxUint32 {
		return 0, fmt.Errorf("device number %d:%d out of range", major, minor)
	}
	return unix.Mkdev(uint32(major), uint32(minor)), nil
}

// xattrUnsupported reports whether an extended attribute call failed because
// the filesystem has no support for them, rather than for a real reason. Where
// that support is per-filesystem rather than per-system - NetBSD has none on
// tmpfs, and the same is true of vfat on Linux - a tree walk would otherwise
// fail on its first entry, so a store with no xattrs to read is treated as a
// store with none rather than as an error.
//
// ENOTSUP and EOPNOTSUPP are the same value on Linux but not on the BSDs, so
// both are tested.
func xattrUnsupported(err error) bool {
	return errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP)
}

// Next returns the next filesystem entry or io.EOF when done. The caller is responsible
// for closing the returned File object.
func (fs *LocalFS) Next() (*File, error) {
	fs.once.Do(func() {
		fs.initForReading()
	})

	entry, ok := <-fs.entries
	if !ok {
		return nil, fs.sErr
	}
	if entry.err != nil {
		return nil, entry.err
	}

	var (
		uid, gid     int
		major, minor uint64
	)
	switch sys := entry.info.Sys().(type) {
	case *syscall.Stat_t:
		uid = int(sys.Uid)
		gid = int(sys.Gid)
		// As with mkdev, the split of dev_t into major/minor is
		// platform-specific. Both accessors mask the value, so the
		// sign-extension of the signed dev_t on Darwin is harmless.
		major = uint64(unix.Major(uint64(sys.Rdev)))
		minor = uint64(unix.Minor(uint64(sys.Rdev)))
	default:
		panic("unsupported platform")
	}

	// Extract the Xattrs if any
	xa := make(map[string]string)
	keys, err := xattr.LList(entry.path)
	if err != nil && !xattrUnsupported(err) {
		return nil, err
	}
	for _, key := range keys {
		value, err := xattr.LGet(entry.path, key)
		if err != nil {
			return nil, err
		}
		xa[key] = string(value)
	}

	// If it's a file, open it and return a ReadCloser
	var r io.ReadCloser
	if entry.info.Mode().IsRegular() {
		data, err := os.Open(entry.path)
		if err != nil {
			return nil, err
		}
		r = data
	}

	// If this is a symlink we need to get the link target
	var linkTarget string
	if entry.info.Mode()&os.ModeSymlink != 0 {
		linkTarget, err = os.Readlink(entry.path)
		if err != nil {
			return nil, err
		}
	}

	mtime := entry.info.ModTime()
	if fs.opts.NoTime {
		mtime = time.Unix(0, 0)
	}

	f := &File{
		Name:       entry.info.Name(),
		Path:       path.Clean(entry.path),
		Mode:       entry.info.Mode(),
		ModTime:    mtime,
		Size:       uint64(entry.info.Size()),
		LinkTarget: linkTarget,
		Uid:        uid,
		Gid:        gid,
		Xattrs:     xa,
		DevMajor:   major,
		DevMinor:   minor,
		Data:       r,
	}

	return f, nil
}

func (fs *LocalFS) initForReading() {
	if fs.opts.OneFileSystem {
		info, err := os.Lstat(fs.Root)
		if err == nil {
			st, ok := info.Sys().(*syscall.Stat_t)
			if ok {
				// Dev (and Rdev) elements of syscall.Stat_t are uint64 on Linux,
				// but int32 on MacOS. Cast it to uint64 everywhere.
				fs.dev = uint64(st.Dev)
			}
		}
	}
	fs.startSerializer()
}

func (fs *LocalFS) startSerializer() {
	go func() {
		err := filepath.Walk(fs.Root, func(path string, info os.FileInfo, err error) error {
			if fs.dev != 0 && info.IsDir() {
				// one-file-system is set, skip other filesystems
				st, ok := info.Sys().(*syscall.Stat_t)
				if ok && uint64(st.Dev) != fs.dev {
					return nil
				}
			}
			fs.entries <- walkEntry{path, info, err}
			return nil
		})
		fs.sErr = err
		if err == nil {
			fs.sErr = io.EOF
		}
		close(fs.entries)
	}()
}
