package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Commands that write an index do so after chunking and uploading, so a name
// the destination can't represent has to be caught before that work.
func TestValidateIndexLocation(t *testing.T) {
	for _, location := range []string{
		"index.caibx",
		"/tmp/index.caibx",
		"oci+https://ghcr.io/user/repo/index.caibx",
		"s3+https://host/bucket/.hidden.caibx",
		"https://host/path/.hidden.caibx",
	} {
		assert.NoError(t, validateIndexLocation(location), location)
	}
	for _, location := range []string{
		"oci+https://ghcr.io/user/repo/.hidden.caibx",
		"oci+http://127.0.0.1:5000/user/repo/with space.caibx",
	} {
		assert.Error(t, validateIndexLocation(location), location)
	}
}
