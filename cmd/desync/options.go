package main

import (
	"errors"

	"github.com/folbricht/desync"
	"github.com/spf13/pflag"
)

// cmdStoreOptions are used to pass additional options to store initalization from the
// commandline. These generally override settings from the config file.
type cmdStoreOptions struct {
	n             int
	clientCert    string
	clientKey     string
	caCert        string
	skipVerify    bool
	trustInsecure bool
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
	if o.caCert != "" {
		opt.CACert = o.caCert
	}
	if o.skipVerify {
		opt.SkipVerify = true
	}
	if o.trustInsecure {
		opt.TrustInsecure = true
	}
	return opt
}

// Validate the command line options are sensical and return an error if they aren't.
func (o cmdStoreOptions) validate() error {
	if (o.clientKey == "") != (o.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	return nil
}

// Add common store option flags to a command flagset.
func addStoreOptions(o *cmdStoreOptions, f *pflag.FlagSet) {
	f.IntVarP(&o.n, "concurrency", "n", 10, "number of concurrent goroutines")
	f.StringVar(&o.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	f.StringVar(&o.clientKey, "client-key", "", "path to client key for TLS authentication")
	f.StringVar(&o.caCert, "ca-cert", "", "trust authorities in this file, instead of OS trust store")
	f.BoolVarP(&o.trustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
}

// cmdServerOptions hold command line options used in HTTP servers.
type cmdServerOptions struct {
	cert      string
	key       string
	mutualTLS bool
	clientCA  string
	auth      string
}

func (o cmdServerOptions) validate() error {
	if (o.key == "") != (o.cert == "") {
		return errors.New("--key and --cert options need to be provided together")
	}
	return nil
}

// Add common HTTP server options to a command flagset.
func addServerOptions(o *cmdServerOptions, f *pflag.FlagSet) {
	f.StringVar(&o.cert, "cert", "", "cert file in PEM format, requires --key")
	f.StringVar(&o.key, "key", "", "key file in PEM format, requires --cert")
	f.BoolVar(&o.mutualTLS, "mutual-tls", false, "require valid client certficate")
	f.StringVar(&o.clientCA, "client-ca", "", "acceptable client certificate or CA")
	f.StringVar(&o.auth, "authorization", "", "expected value of the authorization header in requests")
}
