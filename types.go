package desync

import (
	"encoding/hex"
	"encoding/json"

	"github.com/pkg/errors"
)

// ChunkID is the SHA512/256 in binary encoding
type ChunkID [32]byte

// ChunkIDFromSlice converts a SHA512/256 encoded as byte slice into a ChunkID.
// It's expected the slice is of the correct length
func ChunkIDFromSlice(b []byte) (ChunkID, error) {
	var c ChunkID
	if len(b) != len(c) {
		return c, errors.New("chunk id string not of right size")
	}
	copy(c[:], b)
	return c, nil
}

// ChunkIDFromString converts a SHA512/56 encoded as string into a ChunkID
func ChunkIDFromString(id string) (ChunkID, error) {
	b, err := hex.DecodeString(id)
	if err != nil {
		return ChunkID{}, errors.Wrap(err, "failed to decode chunk id string")
	}
	return ChunkIDFromSlice(b)
}

func (c *ChunkID) String() string {
	return hex.EncodeToString(c[:])
}

func (c *ChunkID) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

func (c *ChunkID) UnmarshalJSON(data []byte) error {
	var (
		str string
		err error
	)
	if err = json.Unmarshal(data, &str); err != nil {
		return err
	}
	*c, err = ChunkIDFromString(str)
	return err
}
