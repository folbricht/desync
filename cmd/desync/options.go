package main

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/pkg/chunkers"
	"github.com/spf13/pflag"
)

// cmdStoreOptions are used to pass additional options to store initialization from the
// commandline. These generally override settings from the config file.
type cmdStoreOptions struct {
	n int
	// Number of goroutines the command runs to make requests to the stores,
	// once it's known. An adaptive store can raise it above n, stores with a
	// fixed concurrency are then held to theirs.
	workers                int
	clientCert             string
	clientKey              string
	caCert                 string
	skipVerify             bool
	trustInsecure          bool
	cacheRepair            bool
	errorRetry             int
	errorRetryBaseInterval time.Duration
	pflag.FlagSet
}

// MergedWith takes store options as read from the config, and applies command-line
// provided options on top of them and returns the merged result.
func (o cmdStoreOptions) MergedWith(opt desync.StoreOptions) desync.StoreOptions {
	// Unlike the options below, the flag carries a default rather than a zero
	// value, so it can't just win when it wasn't given. Take the concurrency
	// from the config unless the flag was set, or the config didn't name a
	// usable one.
	usable := opt.N >= 1 || opt.N == desync.AdaptiveConcurrency
	if !usable || o.FlagSet.Lookup("concurrency").Changed {
		opt.N = o.n
	}

	if o.FlagSet.Lookup("client-cert").Changed {
		opt.ClientCert = o.clientCert
	}
	if o.FlagSet.Lookup("client-key").Changed {
		opt.ClientKey = o.clientKey
	}
	if o.FlagSet.Lookup("ca-cert").Changed {
		opt.CACert = o.caCert
	}
	if o.skipVerify {
		opt.SkipVerify = true
	}
	if o.FlagSet.Lookup("trust-insecure").Changed {
		opt.TrustInsecure = o.trustInsecure
	}
	if o.FlagSet.Lookup("error-retry").Changed {
		opt.ErrorRetry = o.errorRetry
	}
	if o.FlagSet.Lookup("error-retry-base-interval").Changed {
		opt.ErrorRetryBaseInterval = o.errorRetryBaseInterval
	}
	return opt
}

// Validate the command line options are sensical and return an error if they aren't.
func (o cmdStoreOptions) validate() error {
	if (o.clientKey == "") != (o.clientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	if o.n < 1 && o.n != desync.AdaptiveConcurrency {
		// Without workers, nothing reads from the queues the commands feed
		// chunks into, and they'd block forever.
		return errors.New("--concurrency needs to be at least 1, or -1 to let desync choose")
	}
	return nil
}

// storeWorkers returns the number of goroutines a command runs to make
// requests to the stores at these locations. If the concurrency of any remote
// store is adaptive, from the command line or the config, that's enough for
// its limit to reach the maximum. Nothing limits requests to local stores,
// with an adaptive concurrency and no remote store, the work is bound by the
// CPU or local disks.
func (o cmdStoreOptions) storeWorkers(locations ...string) (int, error) {
	for _, location := range locations {
		if location == "" {
			continue
		}
		// Members of a failover group can have options of their own
		for member := range strings.SplitSeq(location, "|") {
			if !isRemoteLocation(member) {
				continue
			}
			opt, err := storeOptionsFor(member, o)
			if err != nil {
				return 0, err
			}
			if opt.N == desync.AdaptiveConcurrency {
				return desync.MaxAdaptiveConcurrency, nil
			}
		}
	}
	return o.cpuWorkers(), nil
}

// cpuWorkers returns the number of goroutines a command runs for work bound
// by the CPU or local disks, like chunking or hashing.
func (o cmdStoreOptions) cpuWorkers() int {
	if o.n == desync.AdaptiveConcurrency {
		n := runtime.GOMAXPROCS(0)
		desync.Log.Debugf("using %d goroutines, one per CPU", n)
		return n
	}
	return o.n
}

// Add common store option flags to a command flagset.
func addStoreOptions(o *cmdStoreOptions, f *pflag.FlagSet) {
	f.IntVarP(&o.n, "concurrency", "n", 10, fmt.Sprintf("number of concurrent goroutines, -1 to let desync choose: the number of CPUs for chunking and hashing, adapting to each store up to %d for transfers", desync.MaxAdaptiveConcurrency))
	f.StringVar(&o.clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	f.StringVar(&o.clientKey, "client-key", "", "path to client key for TLS authentication")
	f.StringVar(&o.caCert, "ca-cert", "", "trust authorities in this file, instead of OS trust store")
	f.BoolVarP(&o.trustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	f.BoolVarP(&o.cacheRepair, "cache-repair", "r", true, "replace invalid chunks in the cache from source")
	f.IntVarP(&o.errorRetry, "error-retry", "e", desync.DefaultErrorRetry, "number of times to retry in case of network error")
	f.DurationVarP(&o.errorRetryBaseInterval, "error-retry-base-interval", "b", desync.DefaultErrorRetryBaseInterval, "initial retry delay, increases linearly with each subsequent attempt")

	o.FlagSet = *f
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
	if o.key == "" {
		if o.mutualTLS {
			return errors.New("--mutual-tls requires --cert and --key (TLS must be enabled)")
		}
		if o.clientCA != "" {
			return errors.New("--client-ca requires --cert and --key (TLS must be enabled)")
		}
	}
	if o.mutualTLS && o.clientCA == "" {
		return errors.New("--mutual-tls requires --client-ca (otherwise any certificate trusted by the system CA pool would be accepted)")
	}
	if o.clientCA != "" && !o.mutualTLS {
		return errors.New("--client-ca requires --mutual-tls (otherwise client certificates are not verified)")
	}
	return nil
}

// Add common HTTP server options to a command flagset.
func addServerOptions(o *cmdServerOptions, f *pflag.FlagSet) {
	f.StringVar(&o.cert, "cert", "", "cert file in PEM format, requires --key")
	f.StringVar(&o.key, "key", "", "key file in PEM format, requires --cert")
	f.BoolVar(&o.mutualTLS, "mutual-tls", false, "require valid client certificate")
	f.StringVar(&o.clientCA, "client-ca", "", "acceptable client certificate or CA")
	f.StringVar(&o.auth, "authorization", "", "expected value of the authorization header in requests")
}

// cmdChunkerOptions hold command line options to configure a cunker
type cmdChunkerOptions struct {
	name    string
	sizes   string
	options string
}

func addChunkerOptions(o *cmdChunkerOptions, f *pflag.FlagSet) {
	f.StringVar(&o.name, "chunker", chunkers.DefaultChunkerName, fmt.Sprintf("chunking algorithm to use, pick from %v", chunkers.RegisteredNames()))
	defaultChunkSizes := fmt.Sprintf("%v:%v:%v",
		chunkers.DefaultChunkSizeMin/1024,
		chunkers.DefaultChunkSizeAvg/1024,
		chunkers.DefaultChunkSizeMax/1024,
	)
	f.StringVarP(&o.sizes, "chunk-size", "m", defaultChunkSizes, "min:avg:max chunk size in kb")
	f.StringVar(&o.options, "chunker-options", "", "additional chunker-specific options")
}

func (o *cmdChunkerOptions) ToChunkingSettings() (result chunkers.ChunkingSettings, err error) {
	result = chunkers.DefaultChunkingSettings()

	if o.name != "" {
		result.ChunkerName = o.name
	}
	if o.sizes != "" {
		result.ChunkerParams.Min, result.ChunkerParams.Avg, result.ChunkerParams.Max, err = parseChunkSizeParam(o.sizes)
		if err != nil {
			return
		}
	}

	if o.options != "" {
		result.ChunkerParams.Options = o.options
	}
	return
}

func parseChunkSizeParam(s string) (min, avg, max uint64, err error) {
	sizes := strings.Split(s, ":")
	if len(sizes) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid chunk size '%s'", s)
	}
	num, err := strconv.Atoi(sizes[0])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "min chunk size")
	}
	min = uint64(num) * 1024
	num, err = strconv.Atoi(sizes[1])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "avg chunk size")
	}
	avg = uint64(num) * 1024
	num, err = strconv.Atoi(sizes[2])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "max chunk size")
	}
	max = uint64(num) * 1024
	return
}
