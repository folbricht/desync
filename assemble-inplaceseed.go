package desync

// InPlaceSeed is a FileSeed where the source and destination are the same file.
// This makes the relationship explicit when desync extract is used with seeds
// that resolve to the same path as the extraction target.
type InPlaceSeed struct {
	*FileSeed
}

// NewInPlaceSeed initializes a seed where the source and destination are the
// same file. It passes the file path as both src and dst to NewFileSeed.
func NewInPlaceSeed(file string, index Index) (*InPlaceSeed, error) {
	fs, err := NewFileSeed(file, file, index)
	if err != nil {
		return nil, err
	}
	return &InPlaceSeed{FileSeed: fs}, nil
}
