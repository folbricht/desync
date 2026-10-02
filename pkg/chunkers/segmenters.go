package chunkers

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

var Segmenters = map[string]SegmenterFunc{
	"csv": readSegmensFromCSV,
}

type SegmenterFunc func(io.Reader) ([]uint64, error)

func readSegmensFromCSV(reader io.Reader) ([]uint64, error) {
	r := csv.NewReader(reader)
	r.ReuseRecord = true

	result := make([]uint64, 0, 1024)
	for {
		strValues, err := r.Read()
		if err == io.EOF {
			return result, nil
		}

		intValue, err := strconv.Atoi(strValues[0])
		if err != nil {
			return nil, fmt.Errorf("fillSegmentSizes: %w", err)
		}
		if intValue <= 0 {
			return nil, fmt.Errorf("fillSegmentSizes: invalid segment size '%v'", strValues[0])
		}
		result = append(result, uint64(intValue))
	}
}
