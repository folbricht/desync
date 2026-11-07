package sshhost

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	valid := []string{
		"example.com",
		"user@example.com",
		"user@example.com:2222",
		"192.0.2.1",
		"a-b.example.com", // dash allowed mid-string
	}
	for _, h := range valid {
		assert.NoError(t, Validate(h), "%q should be accepted", h)
	}

	invalid := []string{
		"-",
		"-x",
		"-oProxyCommand=touch${IFS}pwned",
		"-oProxyCommand=id@example.com",
	}
	for _, h := range invalid {
		assert.Error(t, Validate(h), "%q should be rejected", h)
	}
}
