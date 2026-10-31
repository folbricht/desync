// Package sshhost validates ssh destinations built from store URLs.
package sshhost

import (
	"fmt"
	"strings"
)

// Validate rejects an ssh destination that begins with a dash. Such a value
// (built from the store URL's user and host) would otherwise be parsed by ssh
// as a command-line option rather than a destination, allowing option
// injection such as -oProxyCommand=... and arbitrary local command execution
// (the same class of bug as Git CVE-2017-1000117 and Mercurial CVE-2017-1000116).
// Callers must reject the destination before passing it to ssh; the "--"
// argument added to the ssh invocation is a defense-in-depth measure that only
// some ssh implementations honor.
func Validate(host string) error {
	if strings.HasPrefix(host, "-") {
		return fmt.Errorf("invalid ssh destination %q: must not begin with '-'", host)
	}
	return nil
}
