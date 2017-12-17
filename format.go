package desync

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type FormatHeader struct {
	Size uint64
	Type uint64
}

type FormatEntry struct {
	FormatHeader
	FeatureFlags uint64
	Mode         uint64
	Flags        uint64
	UID          uint64
	GID          uint64
	MTime        uint64
}

type FormatUser struct {
	FormatHeader
	Name string
}

type FormatGroup struct {
	FormatHeader
	Name string
}

type FormatXAttr struct {
	FormatHeader
	NameAndValue string
}

type FormatSELinux struct {
	FormatHeader
	Label string
}

type FormatFilename struct {
	FormatHeader
	Name string
}

type FormatSymlink struct {
	FormatHeader
	Target string
}

type FormatDevice struct {
	FormatHeader
	Major uint64
	Minor uint64
}

type FormatPayload struct {
	FormatHeader
	Data io.Reader
}

type FormatGoodbye struct {
	FormatHeader
	Items []FormatGoodbyeItem
}

type FormatGoodbyeItem struct {
	Offset uint64
	Size   uint64
	Hash   uint64 // The last item in a list has the CaFormatGoodbyeTailMarker here
}

type FormatFCaps struct {
	FormatHeader
	Data []byte
}

type FormatACLUser struct {
	FormatHeader
	UID         uint64
	Permissions uint64
	Name        string
}

type FormatACLGroup struct {
	FormatHeader
	GID         uint64
	Permissions uint64
	Name        string
}

type FormatACLGroupObj struct {
	FormatHeader
	Permissions uint64
}

type FormatACLDefault struct {
	FormatHeader
	UserObjPermissions  uint64
	GroupObjPermissions uint64
	OtherPermissions    uint64
	MaskPermissions     uint64
}

type FormatIndex struct {
	FormatHeader
	FeatureFlags uint64
	ChunkSizeMin uint64
	ChunkSizeAvg uint64
	ChunkSizeMax uint64
}

type FormatTable struct {
	FormatHeader
	Items []FormatTableItem
}

type FormatTableItem struct {
	Offset uint64
	Chunk  ChunkID
}

// FormatDecoder is used to parse and break up a stream of casync format elements
// found in archives or index files.
type FormatDecoder struct {
	r       reader
	advance io.Reader
}

func NewFormatDecoder(r io.Reader) FormatDecoder {
	return FormatDecoder{r: reader{r}}
}

// Next returns the next format element from the stream. If an element
// contains a reader, that reader should be used before any subsequent calls as
// it'll be invalidated then. Returns nil when the end is reached.
func (d *FormatDecoder) Next() (interface{}, error) {
	// If we previously returned a reader, make sure we advance all the way in
	// case the caller didn't read it all.
	if d.advance != nil {
		io.Copy(ioutil.Discard, d.advance)
		d.advance = nil
	}
	hdr, err := d.r.ReadHeader()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	switch hdr.Type {
	case CaFormatEntry:
		if hdr.Size != 64 {
			return nil, InvalidFormat{}
		}
		e := FormatEntry{FormatHeader: hdr}
		e.FeatureFlags, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.Mode, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.Flags, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.UID, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.GID, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.MTime, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		return e, nil

	case CaFormatUser:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		return FormatUser{FormatHeader: hdr, Name: string(b)}, nil

	case CaFormatGroup:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		return FormatGroup{FormatHeader: hdr, Name: string(b)}, nil

	case CaFormatXAttr:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		return FormatXAttr{FormatHeader: hdr, NameAndValue: string(b)}, nil

	case CaFormatSELinux:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		return FormatSELinux{FormatHeader: hdr, Label: string(b)}, nil

	case CaFormatFilename:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		return FormatFilename{FormatHeader: hdr, Name: string(b)}, nil

	case CaFormatSymlink:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		return FormatSymlink{FormatHeader: hdr, Target: string(b)}, nil

	case CaFormatDevice:
		if hdr.Size != 32 {
			return nil, InvalidFormat{}
		}
		e := FormatDevice{FormatHeader: hdr}
		e.Major, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.Minor, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		return e, nil

	case CaFormatPayload:
		size := hdr.Size - 16
		r := io.LimitReader(d.r, int64(size))
		// Record the reader to be read fully on the next iteration if the caller
		// didn't do it
		d.advance = r
		return FormatPayload{FormatHeader: hdr, Data: r}, nil

	case CaFormatFCaps:
		b := make([]byte, hdr.Size-16)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		return FormatFCaps{FormatHeader: hdr, Data: b}, nil

	case CaFormatACLUser:
		e := FormatACLUser{FormatHeader: hdr}
		e.UID, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.Permissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		b := make([]byte, hdr.Size-32)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		e.Name = string(b)
		return e, nil

	case CaFormatACLGroup:
		e := FormatACLGroup{FormatHeader: hdr}
		e.GID, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.Permissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		b := make([]byte, hdr.Size-32)
		if _, err = io.ReadFull(d.r, b); err != nil {
			return nil, err
		}
		// Strip off the 0 byte
		b = b[:len(b)-1]
		e.Name = string(b)
		return e, nil

	case CaFormatACLGroupObj:
		e := FormatACLGroupObj{FormatHeader: hdr}
		e.Permissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		return e, nil

	case CaFormatACLDefault:
		e := FormatACLDefault{FormatHeader: hdr}
		e.UserObjPermissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.GroupObjPermissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.OtherPermissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.MaskPermissions, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		return e, nil

	case CaFormatGoodbye:
		n := (hdr.Size - 16) / 24
		items := make([]FormatGoodbyeItem, n)
		e := FormatGoodbye{FormatHeader: hdr, Items: items}
		for i := uint64(0); i < n; i++ {
			items[i].Offset, err = d.r.ReadUint64()
			if err != nil {
				return nil, err
			}
			items[i].Size, err = d.r.ReadUint64()
			if err != nil {
				return nil, err
			}
			items[i].Hash, err = d.r.ReadUint64()
			if err != nil {
				return nil, err
			}
		}
		// Ensure we have the tail marker in the last item
		if items[len(items)-1].Hash != CaFormatGoodbyeTailMarker {
			return nil, InvalidFormat{"tail marker not found"}
		}
		return e, nil

	case CaFormatIndex:
		e := FormatIndex{FormatHeader: hdr}
		e.FeatureFlags, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.ChunkSizeMin, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.ChunkSizeAvg, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		e.ChunkSizeMax, err = d.r.ReadUint64()
		if err != nil {
			return nil, err
		}
		return e, nil

	case CaFormatTable:
		// The length should be set to MAX_UINT64
		if hdr.Size != math.MaxUint64 {
			return nil, InvalidFormat{"expected size MAX_UINT64 in format table"}
		}

		e := FormatTable{FormatHeader: hdr}
		var items []FormatTableItem
		for {
			offset, err := d.r.ReadUint64()
			if err != nil {
				return nil, err
			}
			if offset == 0 {
				break
			}
			chunk, err := d.r.ReadID()
			if err != nil {
				return nil, err
			}
			items = append(items, FormatTableItem{Offset: offset, Chunk: chunk})
		}
		e.Items = items
		// Confirm that the last element really is the tail marker
		var x uint64
		x, err = d.r.ReadUint64() // zero fill 2
		if err != nil {
			return nil, err
		}
		if x != 0 {
			return nil, InvalidFormat{"tail marker not found"}
		}
		if _, err = d.r.ReadUint64(); err != nil { // index offset
			return nil, err
		}
		if _, err = d.r.ReadUint64(); err != nil { // size
			return nil, err
		}
		x, err = d.r.ReadUint64() // marker
		if err != nil {
			return nil, err
		}
		if x != CaFormatTableTailMarker {
			return nil, InvalidFormat{"tail marker not found"}
		}
		return e, nil

	default:
		return nil, fmt.Errorf("unsupported header type %x", hdr.Type)
	}
}
