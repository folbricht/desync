package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/folbricht/desync"
	"github.com/stretchr/testify/require"
)

func TestInspectChunksCommand(t *testing.T) {
	for _, test := range []struct {
		name               string
		args               []string
		expectedOutputJSON string
	}{
		{"inspect the chunks info with a local store",
			[]string{"-s", "testdata/blob2.store", "testdata/blob2.caibx"},
			"testdata/blob2_chunks_info.json",
		},
		{"run inspect with a seed that doesn't have all the compressed chunks",
			[]string{"-s", "testdata/blob2.cache", "testdata/blob2.caibx"},
			"testdata/blob2_chunks_info_missing.json",
		},
		{"inspect the chunks info without any stores",
			[]string{"testdata/blob2.caibx"},
			"testdata/blob2_chunks_info_no_store.json",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var exp []desync.ChunkAdditionalInfo
			be, err := os.ReadFile(test.expectedOutputJSON)
			require.NoError(t, err)
			err = json.Unmarshal(be, &exp)
			require.NoError(t, err)

			cmd := newinspectChunksCommand(context.Background())
			cmd.SetArgs(test.args)
			b := new(bytes.Buffer)

			// Redirect the command's output
			stdout = b
			cmd.SetOutput(ioutil.Discard)
			_, err = cmd.ExecuteC()
			require.NoError(t, err)

			// Decode the output and compare to what's expected
			var got []desync.ChunkAdditionalInfo
			err = json.Unmarshal(b.Bytes(), &got)
			require.NoError(t, err)
			require.Equal(t, exp, got)
		})
	}
}
