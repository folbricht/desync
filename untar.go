package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
)

// UnTar implements the untar command, decoding a catar file and writing the
// contained tree to a target directory.
func UnTar(ctx context.Context, r io.Reader, dst string) error {
	dec := NewArchiveDecoder(r)
loop:
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		c, err := dec.Next()
		if err != nil {
			return err
		}
		switch n := c.(type) {
		case NodeDirectory:
			err = makeDir(dst, n)
		case NodeFile:
			err = makeFile(dst, n)
		case NodeDevice:
			err = makeDevice(dst, n)
		case NodeSymlink:
			err = makeSymlink(dst, n)
		case nil:
			break loop
		default:
			err = fmt.Errorf("unsupported type %s", reflect.TypeOf(c))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func makeDir(base string, n NodeDirectory) error {
	dst := filepath.Join(base, n.Name)

	// Let's see if there is a dir with the same name already
	if info, err := os.Stat(dst); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", dst)
		}
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		if err := os.Mkdir(dst, n.Mode); err != nil {
			return err
		}
	}
	// The dir exists now, fix the UID/GID
	if err := os.Chown(dst, n.UID, n.GID); err != nil {
		return err
	}
	if err := os.Chtimes(dst, n.MTime, n.MTime); err != nil {
		return err
	}
	return syscall.Chmod(dst, uint32(n.Mode))
}

func makeFile(base string, n NodeFile) error {
	dst := filepath.Join(base, n.Name)

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(f, n.Data); err != nil {
		return err
	}
	if err = f.Chown(n.UID, n.GID); err != nil {
		return err
	}
	if err = os.Chtimes(dst, n.MTime, n.MTime); err != nil {
		return err
	}
	return syscall.Chmod(dst, uint32(n.Mode))
}

func makeSymlink(base string, n NodeSymlink) error {
	dst := filepath.Join(base, n.Name)

	if err := os.Symlink(n.Target, dst); err != nil {
		return err
	}
	// TODO: On Linux, the permissions of the link don't matter so we don't
	// set them here. But they do matter somewhat on Mac, so should probably
	// add some Mac-specific logic for that here.
	// fchmodat() with flag AT_SYMLINK_NOFOLLOW
	return os.Lchown(dst, n.UID, n.GID)
}

func makeDevice(base string, n NodeDevice) error {
	dst := filepath.Join(base, n.Name)

	if err := syscall.Mknod(dst, uint32(n.Mode), int(mkdev(n.Major, n.Minor))); err != nil {
		return err
	}
	if err := os.Chown(dst, n.UID, n.GID); err != nil {
		return err
	}
	if err := os.Chtimes(dst, n.MTime, n.MTime); err != nil {
		return err
	}
	return syscall.Chmod(dst, uint32(n.Mode))
}

func mkdev(major, minor uint64) uint64 {
	dev := (major & 0x00000fff) << 8
	dev |= (major & 0xfffff000) << 32
	dev |= (minor & 0x000000ff) << 0
	dev |= (minor & 0xffffff00) << 12
	return dev
}
