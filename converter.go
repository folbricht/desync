package desync

import "strings"

// Converters are modifiers for chunk data, such as compression or encryption.
// They are used to prepare chunk data for storage, or to read it from storage.
// The order of the conversion layers matters. When plain data is prepared for
// storage, the toStorage method is used in the order the layers are defined.
// To read from storage, the fromStorage method is called for each layer in
// reverse order.
type Converters []converter

// Apply every data converter in the forward direction.
func (s Converters) toStorage(in []byte) ([]byte, error) {
	var (
		b   = in
		err error
	)
	for _, layer := range s {
		b, err = layer.toStorage(b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// Apply the layers backwards.
func (s Converters) fromStorage(in []byte) ([]byte, error) {
	var (
		b   = in
		err error
	)
	for i := len(s) - 1; i >= 0; i-- {
		b, err = s[i].fromStorage(b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// commonPrefix returns the number of leading layers shared between the
// two conversion stacks. Used to determine the difference between them,
// for example a compressed store being served encrypted, where only the
// differing layers need to be applied.
func (s Converters) commonPrefix(c Converters) int {
	var n int
	for n < len(s) && n < len(c) && s[n].equal(c[n]) {
		n++
	}
	return n
}

// Extension to be used in storage. Concatenation of converter
// extensions in order (towards storage).
func (s Converters) storageExtension() string {
	var ext strings.Builder
	for _, layer := range s {
		ext.WriteString(layer.storageExtension())
	}
	return ext.String()
}

// converter is a storage data modifier layer.
type converter interface {
	// Convert data from its original form to storage format.
	// The input could be plain data, or the output of a prior
	// converter.
	toStorage([]byte) ([]byte, error)

	// Convert data from its storage format towards its plain
	// form. The input could be encrypted or compressed, while
	// the output may be used for the next conversion layer.
	fromStorage([]byte) ([]byte, error)

	// Returns the file extension that should be used for a
	// chunk when stored. Usually a concatenation of layers.
	storageExtension() string

	// True is one converter matches another exactly.
	equal(converter) bool
}

// Compression layer converter. Compresses/decompresses chunk data
// to and from storage. Implements the converter interface. Lives in
// this file rather than compress.go so it is part of both compression
// build variants.
type Compressor struct{}

var _ converter = Compressor{}

func (d Compressor) toStorage(in []byte) ([]byte, error) {
	return Compress(in)
}

func (d Compressor) fromStorage(in []byte) ([]byte, error) {
	return Decompress(nil, in)
}

func (d Compressor) equal(c converter) bool {
	_, ok := c.(Compressor)
	return ok
}

func (d Compressor) storageExtension() string {
	return ".cacnk"
}
