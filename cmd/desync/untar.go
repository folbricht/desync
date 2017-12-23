package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"github.com/folbricht/desync"
)

const untarUsage = `desync untar <catar> <target>

Extracts a directory tree from a catar file.`

func untar(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("untar", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, untarUsage)
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() < 2 {
		return errors.New("Not enough arguments. See -h for help.")
	}
	if flags.NArg() > 2 {
		return errors.New("Too many arguments. See -h for help.")
	}

	catarFile := flags.Arg(0)
	targetDir := flags.Arg(1)

	f, err := os.Open(catarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	d := desync.NewArchiveDecoder(f)
loop:
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			break
		default:
		}
		c, err := d.Next()
		if err != nil {
			return err
		}
		switch d := c.(type) {
		case desync.NodeDirectory:
			err = makeDir(targetDir, d)
		case desync.NodeFile:
			err = makeFile(targetDir, d)
		case desync.NodeDevice:
			err = makeDevice(targetDir, d)
		case desync.NodeSymlink:
			err = makeSymlink(targetDir, d)
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

func makeDir(base string, d desync.NodeDirectory) error {
	dst := filepath.Join(base, d.Name)

	// Let's see if there is a dir with the same name already
	if info, err := os.Stat(dst); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", dst)
		}
		// Ok the dir exists, let's set the mode anyway
		if err := os.Chmod(dst, d.Mode); err != nil {
			return err
		}
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		if err := os.Mkdir(dst, d.Mode); err != nil {
			return err
		}
	}
	// The dir exists now, fix the UID/GID
	if err := os.Chown(dst, d.UID, d.GID); err != nil {
		return err
	}
	return os.Chtimes(dst, d.MTime, d.MTime)
}

func makeFile(base string, d desync.NodeFile) error {
	dst := filepath.Join(base, d.Name)

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(f, d.Data); err != nil {
		return err
	}
	if err = f.Chmod(d.Mode); err != nil {
		return err
	}
	if err = f.Chown(d.UID, d.GID); err != nil {
		return err
	}
	return os.Chtimes(dst, d.MTime, d.MTime)
}

func makeSymlink(base string, d desync.NodeSymlink) error {
	dst := filepath.Join(base, d.Name)

	if err := os.Symlink(d.Target, dst); err != nil {
		return err
	}
	// TODO: On Linux, the permissions of the link don't matter so we don't
	// set them here. But they do matter somewhat on Mac, so should probably
	// add some Mac-specific logic for that here.
	// fchmodat() with flag AT_SYMLINK_NOFOLLOW
	return os.Lchown(dst, d.UID, d.GID)
}

func makeDevice(base string, d desync.NodeDevice) error {
	dst := filepath.Join(base, d.Name)

	if err := syscall.Mknod(dst, uint32(d.Mode), int(mkdev(d.Major, d.Minor))); err != nil {
		return err
	}
	if err := os.Chown(dst, d.UID, d.GID); err != nil {
		return err
	}
	return os.Chtimes(dst, d.MTime, d.MTime)
}

func mkdev(major, minor uint64) uint64 {
	dev := (major & 0x00000fff) << 8
	dev |= (major & 0xfffff000) << 32
	dev |= (minor & 0x000000ff) << 0
	dev |= (minor & 0xffffff00) << 12
	return dev
}
