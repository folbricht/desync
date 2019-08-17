package desync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type Xattrs map[string]string

// NodeDirectory represents a directory in a catar archive
type NodeDirectory struct {
	Name   string
	UID    int
	GID    int
	Mode   os.FileMode
	MTime  time.Time
	Xattrs Xattrs
}

// NodeFile holds file permissions and data in a catar archive
type NodeFile struct {
	UID    int
	GID    int
	Mode   os.FileMode
	Name   string
	MTime  time.Time
	Xattrs Xattrs
	Size   uint64
	Data   io.Reader
}

// NodeSymlink holds symlink information in a catar archive
type NodeSymlink struct {
	Name   string
	UID    int
	GID    int
	Mode   os.FileMode
	MTime  time.Time
	Xattrs Xattrs
	Target string
}

// NodeDevice holds device information in a catar archive
type NodeDevice struct {
	Name   string
	UID    int
	GID    int
	Mode   os.FileMode
	Major  uint64
	Minor  uint64
	Xattrs Xattrs
	MTime  time.Time
}

// ArchiveDecoder is used to decode a catar archive.
type ArchiveDecoder struct {
	d    FormatDecoder
	dir  string
	last interface{}
}

// NewArchiveDecoder initializes a decoder for a catar archive.
func NewArchiveDecoder(r io.Reader) ArchiveDecoder {
	return ArchiveDecoder{d: NewFormatDecoder(r), dir: "."}
}

// Next returns a node from an archive, or nil if the end is reached. If NodeFile
// is returned, the caller should read the file body before calling Next() again
// as that invalidates the reader.
func (a *ArchiveDecoder) Next() (interface{}, error) {
	var (
		entry   *FormatEntry
		payload *FormatPayload
		symlink *FormatSymlink
		device  *FormatDevice
		xattrs  map[string]string
		name    string
		c       interface{}
		err     error
	)

loop:
	for {
		// First process any elements left over from the last loop before reading
		// new ones from the decoder
		if a.last != nil {
			c = a.last
			a.last = nil
		} else {
			c, err = a.d.Next()
			if err != nil {
				return nil, err
			}
		}

		switch d := c.(type) {
		case FormatEntry:
			if entry != nil {
				return nil, InvalidFormat{}
			}
			entry = &d
		case FormatUser: // Not supported yet
		case FormatGroup:
		case FormatSELinux:
		case FormatACLUser:
		case FormatACLGroup:
		case FormatACLGroupObj:
		case FormatACLDefault:
		case FormatFCaps:
		case FormatPayload:
			if entry == nil {
				return nil, InvalidFormat{}
			}
			payload = &d
			break loop
		case FormatXAttr:
			idx := strings.IndexRune(d.NameAndValue, '\000')
			if entry == nil || idx == -1 {
				return nil, InvalidFormat{}
			}
			if xattrs == nil {
				xattrs = make(map[string]string)
			}
			xattrs[d.NameAndValue[0:idx]] = d.NameAndValue[idx+1:]
		case FormatSymlink:
			if entry == nil {
				return nil, InvalidFormat{}
			}
			symlink = &d
		case FormatDevice:
			if entry == nil {
				return nil, InvalidFormat{}
			}
			device = &d
		case FormatFilename:
			if entry != nil { // Store and come back to it in the next iteration
				a.last = c
				break loop
			}
			name = d.Name
		case FormatGoodbye: // This will effectively be a "cd .."
			if entry != nil {
				a.last = c
				break loop
			}
			a.dir = filepath.Dir(a.dir)
		case nil:
			return nil, nil

		default:
			return nil, fmt.Errorf("unsupported element %s in archive", reflect.TypeOf(d))
		}
	}

	// If it doesn't have a payload or is a device/symlink, it must be a directory
	if payload == nil && device == nil && symlink == nil {
		a.dir = filepath.Join(a.dir, name)
		return NodeDirectory{
			Name:   a.dir,
			UID:    entry.UID,
			GID:    entry.GID,
			Mode:   entry.Mode,
			MTime:  entry.MTime,
			Xattrs: xattrs,
		}, nil
	}

	// Regular file
	if payload != nil {
		return NodeFile{
			Name:   filepath.Join(a.dir, name),
			UID:    entry.UID,
			GID:    entry.GID,
			Mode:   entry.Mode,
			MTime:  entry.MTime,
			Xattrs: xattrs,
			Size:   payload.Size - 16,
			Data:   payload.Data,
		}, nil
	}

	// Device
	if device != nil {
		return NodeDevice{
			Name:   filepath.Join(a.dir, name),
			UID:    entry.UID,
			GID:    entry.GID,
			Mode:   entry.Mode,
			MTime:  entry.MTime,
			Xattrs: xattrs,
			Major:  device.Major,
			Minor:  device.Minor,
		}, nil
	}

	// Symlink
	if symlink != nil {
		return NodeSymlink{
			Name:   filepath.Join(a.dir, name),
			UID:    entry.UID,
			GID:    entry.GID,
			Mode:   entry.Mode,
			MTime:  entry.MTime,
			Xattrs: xattrs,
			Target: symlink.Target,
		}, nil
	}

	return nil, nil
}
