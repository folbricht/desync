package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfoCommand(t *testing.T) {
	for _, test := range []struct {
		name           string
		args           []string
		expectedOutput []byte
	}{
		{"info command with store",
			[]string{"-s", "testdata/blob1.store", "testdata/blob1.caibx"},
			[]byte(`{
				"total": 161,
				"unique": 131,
				"in-store": 131,
				"in-seed": 0,
				"in-cache": 0,
				"not-in-seed-nor-cache": 131,
				"size": 2097152,
				"dedup-size-not-in-seed": 1114112,
				"dedup-size-not-in-seed-nor-cache": 1114112,
				"chunk-size-min": 2048,
				"chunk-size-avg": 8192,
				"chunk-size-max": 32768
			}`)},
		{"info command with seed",
			[]string{"-s", "testdata/blob1.store", "--seed", "testdata/blob2.caibx", "testdata/blob1.caibx"},
			[]byte(`{
				"total": 161,
				"unique": 131,
				"in-store": 131,
				"in-seed": 124,
				"in-cache": 0,
				"not-in-seed-nor-cache": 7,
				"size": 2097152,
				"dedup-size-not-in-seed": 80029,
				"dedup-size-not-in-seed-nor-cache": 80029,
				"chunk-size-min": 2048,
				"chunk-size-avg": 8192,
				"chunk-size-max": 32768
			}`)},
		{"info command with seed and cache",
			[]string{"-s", "testdata/blob2.store", "--seed", "testdata/blob1.caibx", "--cache", "testdata/blob2.cache", "testdata/blob2.caibx"},
			[]byte(`{
				"total": 161,
				"unique": 131,
				"in-store": 131,
				"in-seed": 124,
				"in-cache": 18,
				"not-in-seed-nor-cache": 5,
				"size": 2097152,
				"dedup-size-not-in-seed": 80029,
				"dedup-size-not-in-seed-nor-cache": 67099,
				"chunk-size-min": 2048,
				"chunk-size-avg": 8192,
				"chunk-size-max": 32768
			}`)},
		{"info command with cache",
			[]string{"-s", "testdata/blob2.store", "--cache", "testdata/blob2.cache", "testdata/blob2.caibx"},
			[]byte(`{
				"total": 161,
				"unique": 131,
				"in-store": 131,
				"in-seed": 0,
				"in-cache": 18,
				"not-in-seed-nor-cache": 113,
				"size": 2097152,
				"dedup-size-not-in-seed": 1114112,
				"dedup-size-not-in-seed-nor-cache": 950410,
				"chunk-size-min": 2048,
				"chunk-size-avg": 8192,
				"chunk-size-max": 32768
			}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			exp := make(map[string]interface{})
			err := json.Unmarshal(test.expectedOutput, &exp)
			require.NoError(t, err)

			cmd := newInfoCommand(context.Background())
			cmd.SetArgs(test.args)
			b := new(bytes.Buffer)

			// Redirect the command's output
			stdout = b
			cmd.SetOutput(ioutil.Discard)
			_, err = cmd.ExecuteC()
			require.NoError(t, err)

			// Decode the output and compare to what's expected
			got := make(map[string]interface{})
			err = json.Unmarshal(b.Bytes(), &got)
			require.NoError(t, err)
			require.Equal(t, exp, got)
		})
	}
}
