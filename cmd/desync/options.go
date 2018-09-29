package main

import "github.com/folbricht/desync"

// cmdStoreOptions are used to pass additional options to store initalization from the
// commandline. These generally override settings from the config file.
type cmdStoreOptions struct {
	n          int
	clientCert string
	clientKey  string
	skipVerify bool
}

// MergeWith takes store options as read from the config, and applies command-line
// provided options on top of them and returns the merged result.
func (o cmdStoreOptions) MergedWith(opt desync.StoreOptions) desync.StoreOptions {
	opt.N = o.n
	if o.clientCert != "" {
		opt.ClientCert = o.clientCert
	}
	if o.clientKey != "" {
		opt.ClientKey = o.clientKey
	}
	if o.skipVerify {
		opt.SkipVerify = true
	}
	return opt
}
