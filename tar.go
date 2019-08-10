// +build !windows

package desync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/xattr"
)

// TarFeatureFlags are used as feature flags in the header of catar archives. These
// should be used in index files when chunking a catar as well. TODO: Find out what
// CaFormatWithPermissions is as that's not set incasync-produced catar archives.
const TarFeatureFlags uint64 = CaFormatWith32BitUIDs |
	CaFormatWithNSecTime |
	CaFormatWithPermissions |
	CaFormatWithSymlinks |
	CaFormatWithDeviceNodes |
	CaFormatWithFIFOs |
	CaFormatWithSockets |
	CaFormatWithXattrs |
	CaFormatSHA512256 |
	CaFormatExcludeNoDump

// Tar implements the tar command which recursively parses a directory tree,
// and produces a stream of encoded casync format elements (catar file).
func Tar(ctx context.Context, w io.Writer, src string, oneFileSystem bool) error {
	enc := NewFormatEncoder(w)
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	dev := uint64(0)
	if oneFileSystem {
		st, ok := info.Sys().(*syscall.Stat_t)
		if ok {
			// Dev (and Rdev) elements of syscall.Stat_t are uint64 on Linux, but int32 on MacOS. Cast it to uint64 everywhere.
			dev = uint64(st.Dev)
		}
	}
	_, err = tar(ctx, enc, src, info, dev)
	return err
}

func tar(ctx context.Context, enc FormatEncoder, path string, info os.FileInfo, dev uint64) (n int64, err error) {
	// See if we're meant to stop
	select {
	case <-ctx.Done():
		return n, Interrupted{}
	default:
	}

	// Get the UID/GID and major/minor for devices from the low-level stat structure
	var (
		uid, gid     int
		major, minor uint64
		mode         uint32
	)

	switch sys := info.Sys().(type) {
	case *syscall.Stat_t:
		uid = int(sys.Uid)
		gid = int(sys.Gid)
		major = uint64((sys.Rdev >> 8) & 0xfff)
		minor = (uint64(sys.Rdev) % 256) | ((uint64(sys.Rdev) & 0xfff00000) >> 12)
		mode = uint32(sys.Mode)
	default:
		// TODO What should be done here on platforms that don't support this (Windows)?
		// Default UID/GID to 0 and move on or error?
		return n, errors.New("unsupported platform")
	}
	m := info.Mode()

	// Skip (and warn about) things we can't encode properly
	if !(m.IsDir() || m.IsRegular() || isSymlink(m) || isDevice(m)) {
		fmt.Fprintf(os.Stderr, "skipping '%s' : unsupported node type\n", path)
		return 0, nil
	}

	// CaFormatEntry
	entry := FormatEntry{
		FormatHeader: FormatHeader{Size: 64, Type: CaFormatEntry},
		FeatureFlags: TarFeatureFlags,
		UID:          uid,
		GID:          gid,
		Mode:         os.FileMode(mode),
		MTime:        info.ModTime(),
	}
	nn, err := enc.Encode(entry)
	n += nn
	if err != nil {
		return n, err
	}

	// CaFormatXattrs - Write extended attributes elements
	keys, err := xattr.LList(filepath.Join(path))
	if err != nil {
		return n, err
	}
	for _, key := range keys {
		value, err := xattr.LGet(filepath.Join(path), key)
		if err != nil {
			return n, err
		}
		x := FormatXAttr{
			FormatHeader: FormatHeader{Size: uint64(len(key)) + 1 + uint64(len(value)) + 1 + 16, Type: CaFormatXAttr},
			NameAndValue: key + "\000" + string(value),
		}

		nn, err = enc.Encode(x)
		n += nn
		if err != nil {
			return n, err
		}
	}

	switch {
	case m.IsDir():
		stats, err := ioutil.ReadDir(path)
		if err != nil {
			return n, err
		}
		var items []FormatGoodbyeItem
		for _, s := range stats {
			if dev != 0 {
				// one-file-system is set, skip other filesystems
				st, ok := s.Sys().(*syscall.Stat_t)
				if !ok || uint64(st.Dev) != dev {
					continue
				}
			}

			start := n
			// CaFormatFilename - Write the filename element, then recursively encode
			// the items in the directory
			filename := FormatFilename{
				FormatHeader: FormatHeader{Size: uint64(16 + len(s.Name()) + 1), Type: CaFormatFilename},
				Name:         s.Name(),
			}
			nn, err = enc.Encode(filename)
			n += nn
			if err != nil {
				return n, err
			}
			nn, err = tar(ctx, enc, filepath.Join(path, s.Name()), s, dev)
			n += nn
			if err != nil {
				return n, err
			}

			items = append(items, FormatGoodbyeItem{
				Offset: uint64(start), // This is tempoary, it needs to be re-calculated later as offset from the goodbye marker
				Size:   uint64(n - start),
				Hash:   SipHash([]byte(s.Name())),
			})
		}

		// Fix the offsets in the item list, it needs to be the offset (backwards)
		// from the start of FormatGoodbye
		for i := range items {
			items[i].Offset = uint64(n) - items[i].Offset
		}

		// Turn the list of Goodbye items into a complete BST
		items = makeGoodbyeBST(items)

		// Append the tail marker
		items = append(items, FormatGoodbyeItem{
			Offset: uint64(n),
			Size:   uint64(16 + len(items)*24 + 24),
			Hash:   CaFormatGoodbyeTailMarker,
		})

		// Build the complete goodbye element and encode it
		goodbye := FormatGoodbye{
			FormatHeader: FormatHeader{Size: uint64(16 + len(items)*24), Type: CaFormatGoodbye},
			Items:        items,
		}
		nn, err = enc.Encode(goodbye)
		n += nn
		if err != nil {
			return n, err
		}

	case m.IsRegular():
		f, err := os.Open(path)
		if err != nil {
			return n, err
		}
		defer f.Close()
		payload := FormatPayload{
			FormatHeader: FormatHeader{Size: 16 + uint64(info.Size()), Type: CaFormatPayload},
			Data:         f,
		}
		nn, err = enc.Encode(payload)
		n += nn
		if err != nil {
			return n, err
		}

	case isSymlink(m):
		target, err := os.Readlink(path)
		if err != nil {
			return n, err
		}
		symlink := FormatSymlink{
			FormatHeader: FormatHeader{Size: uint64(16 + len(target) + 1), Type: CaFormatSymlink},
			Target:       target,
		}
		nn, err = enc.Encode(symlink)
		n += nn
		if err != nil {
			return n, err
		}

	case isDevice(m):
		device := FormatDevice{
			FormatHeader: FormatHeader{Size: 32, Type: CaFormatDevice},
			Major:        major,
			Minor:        minor,
		}
		nn, err := enc.Encode(device)
		n += nn
		if err != nil {
			return n, err
		}

	default:
		return n, fmt.Errorf("unable to determine node type of '%s'", path)
	}
	return
}
