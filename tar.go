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
)

// TODO: Find out what CaFormatWithPermissions is as that's not set in
// casync-produced catar archives
const DesyncTarFeatureFlags uint64 = CaFormatWith32BitUIDs |
		CaFormatWithNSecTime |
		CaFormatWithPermissions |
		CaFormatWithSymlinks |
		CaFormatWithDeviceNodes |
		CaFormatWithFIFOs |
		CaFormatWithSockets |
		CaFormatSHA512256 |
		CaFormatExcludeNoDump

// Tar implements the tar command which recursively parses a directory tree,
// and produces a stream of encoded casync format elements (catar file).
func Tar(ctx context.Context, w io.Writer, src string) error {
	enc := NewFormatEncoder(w)
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	_, err = tar(ctx, enc, src, info)
	return err
}

func tar(ctx context.Context, enc FormatEncoder, path string, info os.FileInfo) (n int64, err error) {
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
		major = uint64(sys.Rdev / 256)
		minor = uint64(sys.Rdev % 256)
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
		FeatureFlags: DesyncTarFeatureFlags,
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

	switch {
	case m.IsDir():
		stats, err := ioutil.ReadDir(path)
		if err != nil {
			return n, err
		}
		var items []FormatGoodbyeItem
		for _, s := range stats {
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

			nn, err = tar(ctx, enc, filepath.Join(path, s.Name()), s)
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
		// from the start of the goodbye element, not offset from the start of the stream
		for i := range items {
			items[i].Offset = uint64(n) - items[i].Offset
		}

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

func isSymlink(m os.FileMode) bool {
	return m&os.ModeSymlink != 0
}

func isDevice(m os.FileMode) bool {
	return m&os.ModeDevice != 0
}
