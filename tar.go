// +build !windows

package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
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
	CaFormatExcludeNoDump |
	CaFormatExcludeFile

// Tar implements the tar command which recursively parses a directory tree,
// and produces a stream of encoded casync format elements (catar file).
func Tar(ctx context.Context, w io.Writer, fs FilesystemReader) error {
	enc := NewFormatEncoder(w)
	buf := &fsBufReader{fs, nil}
	_, err := tar(ctx, enc, buf, nil)
	return err
}

func tar(ctx context.Context, enc FormatEncoder, fs *fsBufReader, f *File) (n int64, err error) {
	// See if we're meant to stop
	select {
	case <-ctx.Done():
		return n, Interrupted{}
	default:
	}

	// Read very first entry
	if f == nil {
		f, err := fs.Next()
		if err != nil {
			return 0, err
		}
		return tar(ctx, enc, fs, f)
	}

	// Skip (and warn about) things we can't encode properly
	if !(f.IsDir() || f.IsRegular() || f.IsSymlink() || f.IsDevice()) {
		fmt.Fprintf(os.Stderr, "skipping '%s' : unsupported node type\n", f.Name)
		return 0, nil
	}

	// CaFormatEntry
	entry := FormatEntry{
		FormatHeader: FormatHeader{Size: 64, Type: CaFormatEntry},
		FeatureFlags: TarFeatureFlags,
		UID:          f.Uid,
		GID:          f.Gid,
		Mode:         f.Mode,
		MTime:        f.ModTime,
	}
	nn, err := enc.Encode(entry)
	n += nn
	if err != nil {
		return n, err
	}

	// CaFormatXattrs - Write extended attributes elements. These have to be sorted by key.
	keys := make([]string, 0, len(f.Xattrs))
	for key := range f.Xattrs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := f.Xattrs[key]
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
	case f.IsDir():
		dir := f.Path

		var items []FormatGoodbyeItem
		for {
			f, err := fs.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return n, err
			}

			// End of the current dir?
			if !strings.HasPrefix(f.Path, dir) {
				fs.Buffer(f)
				break
			}

			start := n
			// CaFormatFilename - Write the filename element, then recursively encode
			// the items in the directory
			name := path.Base(f.Name)
			filename := FormatFilename{
				FormatHeader: FormatHeader{Size: uint64(16 + len(name) + 1), Type: CaFormatFilename},
				Name:         name,
			}
			nn, err = enc.Encode(filename)
			n += nn
			if err != nil {
				return n, err
			}
			nn, err = tar(ctx, enc, fs, f)
			n += nn
			if err != nil {
				return n, err
			}

			items = append(items, FormatGoodbyeItem{
				Offset: uint64(start), // This is tempoary, it needs to be re-calculated later as offset from the goodbye marker
				Size:   uint64(n - start),
				Hash:   SipHash([]byte(name)),
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

	case f.IsRegular():
		defer f.Close()
		payload := FormatPayload{
			FormatHeader: FormatHeader{Size: 16 + uint64(f.Size), Type: CaFormatPayload},
			Data:         f.Data,
		}
		nn, err = enc.Encode(payload)
		n += nn
		if err != nil {
			return n, err
		}

	case f.IsSymlink():
		symlink := FormatSymlink{
			FormatHeader: FormatHeader{Size: uint64(16 + len(f.LinkTarget) + 1), Type: CaFormatSymlink},
			Target:       f.LinkTarget,
		}
		nn, err = enc.Encode(symlink)
		n += nn
		if err != nil {
			return n, err
		}

	case f.IsDevice():
		device := FormatDevice{
			FormatHeader: FormatHeader{Size: 32, Type: CaFormatDevice},
			Major:        f.DevMajor,
			Minor:        f.DevMinor,
		}
		nn, err := enc.Encode(device)
		n += nn
		if err != nil {
			return n, err
		}

	default:
		return n, fmt.Errorf("unable to determine node type of '%s'", f.Name)
	}
	return
}

// Wrapper for filesystem reader to allow returning elements into a buffer
type fsBufReader struct {
	fs  FilesystemReader
	buf *File
}

func (b *fsBufReader) Next() (*File, error) {
	if b.buf != nil {
		f := b.buf
		b.buf = nil
		return f, nil
	}
	return b.fs.Next()
}

func (b *fsBufReader) Buffer(f *File) {
	if b.buf != nil {
		panic("can only unbuffer one file")
	}
	b.buf = f
}
