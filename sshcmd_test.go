package desync

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSSHHost(t *testing.T) {
	valid := []string{
		"example.com",
		"user@example.com",
		"user@example.com:2222",
		"192.0.2.1",
		"a-b.example.com", // dash allowed mid-string
	}
	for _, h := range valid {
		assert.NoError(t, validateSSHHost(h), "%q should be accepted", h)
	}

	invalid := []string{
		"-",
		"-x",
		"-oProxyCommand=touch${IFS}pwned",
		"-oProxyCommand=id@example.com",
	}
	for _, h := range invalid {
		assert.Error(t, validateSSHHost(h), "%q should be rejected", h)
	}
}

func TestShellQuote(t *testing.T) {
	// Each input must survive a round-trip through a POSIX shell unchanged,
	// proving the quoting can't be broken out of.
	inputs := []string{
		"",
		"/path/to/store/",
		"plain",
		"o'brien",
		`a'; touch pwned; '`,
		`'`,
		`''`,
		`back\slash`,
		`$VAR ${IFS} "dq" |&;<>`,
	}
	for _, in := range inputs {
		out, err := exec.Command("sh", "-c", "printf %s "+shellQuote(in)).Output()
		require.NoError(t, err, "input %q", in)
		assert.Equal(t, in, string(out), "input %q did not round-trip", in)
	}
}
