package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const defaultErrorRetry = 3
const DefaultErrorRetryBaseInterval = 500 * time.Millisecond

func newTestOptionsCommand(opt *cmdStoreOptions) *cobra.Command {
	cmd := &cobra.Command{}

	addStoreOptions(opt, cmd.Flags())
	return cmd
}

func TestErrorRetryOptions(t *testing.T) {
	for _, test := range []struct {
		name                  string
		args                  []string
		cfgFileContent        []byte
		errorRetryStoreHit    int
		errorRetryStoreMiss   int
		baseIntervalStoreHit  time.Duration
		baseIntervalStoreMiss time.Duration
	}{
		{"Config with the error retry and base interval set",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20, "error-retry-base-interval": 250000000}}}`),
			20, defaultErrorRetry, 250000000, DefaultErrorRetryBaseInterval,
		},
		{"Error retry and base interval via command line args",
			[]string{"--error-retry", "10", "--error-retry-base-interval", "1s"},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20, "error-retry-base-interval": 250000000}}}`),
			10, 10, 1000000000, 1000000000,
		},
		{"Config without error retry nor base interval",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"uncompressed": true}}}`),
			defaultErrorRetry, defaultErrorRetry, DefaultErrorRetryBaseInterval, DefaultErrorRetryBaseInterval,
		},
		{"Config with default error retry and base interval",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 3, "error-retry-base-interval": 500000000}}}`),
			defaultErrorRetry, defaultErrorRetry, DefaultErrorRetryBaseInterval, DefaultErrorRetryBaseInterval,
		},
		{"Config that disables error retry and base interval",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 0, "error-retry-base-interval": 0}}}`),
			0, defaultErrorRetry, 0, DefaultErrorRetryBaseInterval,
		},
		{"Disables error retry and base interval via command line args",
			[]string{"--error-retry", "0", "--error-retry-base-interval", "0"},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20, "error-retry-base-interval": 250000000}}}`),
			0, 0, 0, 0,
		},
		{"Force the default values via command line args",
			[]string{"--error-retry", "3", "--error-retry-base-interval", "500ms"},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20, "error-retry-base-interval": 750000000}}}`),
			defaultErrorRetry, defaultErrorRetry, DefaultErrorRetryBaseInterval, DefaultErrorRetryBaseInterval,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "desync-options")
			require.NoError(t, os.WriteFile(f, test.cfgFileContent, 0644))

			// Set the global config file name
			cfgFile = f

			initConfig()

			var cmdOpt cmdStoreOptions

			cmd := newTestOptionsCommand(&cmdOpt)
			cmd.SetArgs(test.args)

			// Execute the mock command, to load the options provided in the launch arguments
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			configOptions, err := cfg.GetStoreOptionsFor("/store/20230901")
			require.NoError(t, err)
			opt := cmdOpt.MergedWith(configOptions)
			require.Equal(t, test.errorRetryStoreHit, opt.ErrorRetry)
			require.Equal(t, test.baseIntervalStoreHit, opt.ErrorRetryBaseInterval)

			configOptions, err = cfg.GetStoreOptionsFor("/missingStore")
			opt = cmdOpt.MergedWith(configOptions)
			require.NoError(t, err)
			require.Equal(t, test.errorRetryStoreMiss, opt.ErrorRetry)
			require.Equal(t, test.baseIntervalStoreMiss, opt.ErrorRetryBaseInterval)
		})
	}
}

func TestStoreOptionsValidate(t *testing.T) {
	for _, test := range []struct {
		name    string
		opt     cmdStoreOptions
		wantErr string
	}{
		{"defaults", cmdStoreOptions{n: 10}, ""},
		{"TLS client auth", cmdStoreOptions{n: 10, clientCert: "c", clientKey: "k"}, ""},
		{"key without cert", cmdStoreOptions{n: 10, clientKey: "k"}, "--client-key and --client-cert"},
		{"cert without key", cmdStoreOptions{n: 10, clientCert: "c"}, "--client-key and --client-cert"},
		{"zero concurrency", cmdStoreOptions{n: 0}, "--concurrency"},
		{"negative concurrency", cmdStoreOptions{n: -1}, "--concurrency"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.opt.validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

// The concurrency flag carries a default rather than a zero value, so the
// merge has to tell "not given" from "given as 10" to let a config file set it.
func TestConcurrencyOption(t *testing.T) {
	const defaultConcurrency = 10
	for _, test := range []struct {
		name           string
		args           []string
		cfgFileContent []byte
		storeHit       int
		storeMiss      int
	}{
		{"config sets the concurrency",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"n": 50}}}`),
			50, defaultConcurrency,
		},
		{"the flag overrides the config",
			[]string{"--concurrency", "20"},
			[]byte(`{"store-options": {"/store/*/":{"n": 50}}}`),
			20, 20,
		},
		{"the flag set to the default still overrides",
			[]string{"--concurrency", "10"},
			[]byte(`{"store-options": {"/store/*/":{"n": 50}}}`),
			defaultConcurrency, defaultConcurrency,
		},
		{"config without a concurrency",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"uncompressed": true}}}`),
			defaultConcurrency, defaultConcurrency,
		},
		{"a config concurrency no store could use is ignored",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"n": 0}}}`),
			defaultConcurrency, defaultConcurrency,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "desync-options")
			require.NoError(t, os.WriteFile(f, test.cfgFileContent, 0644))
			cfgFile = f
			initConfig()

			var cmdOpt cmdStoreOptions
			cmd := newTestOptionsCommand(&cmdOpt)
			cmd.SetArgs(test.args)
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			configOptions, err := cfg.GetStoreOptionsFor("/store/20230901")
			require.NoError(t, err)
			require.Equal(t, test.storeHit, cmdOpt.MergedWith(configOptions).N)

			configOptions, err = cfg.GetStoreOptionsFor("/missingStore")
			require.NoError(t, err)
			require.Equal(t, test.storeMiss, cmdOpt.MergedWith(configOptions).N)
		})
	}
}

func TestServerOptionsValidate(t *testing.T) {
	for _, test := range []struct {
		name    string
		opt     cmdServerOptions
		wantErr string
	}{
		{"no TLS, no mTLS", cmdServerOptions{}, ""},
		{"TLS only", cmdServerOptions{cert: "c", key: "k"}, ""},
		{"TLS with mutualTLS and clientCA", cmdServerOptions{cert: "c", key: "k", mutualTLS: true, clientCA: "ca"}, ""},
		{"key without cert", cmdServerOptions{key: "k"}, "--key and --cert"},
		{"cert without key", cmdServerOptions{cert: "c"}, "--key and --cert"},
		{"mutualTLS without TLS", cmdServerOptions{mutualTLS: true}, "--mutual-tls requires --cert and --key"},
		{"mutualTLS without clientCA", cmdServerOptions{cert: "c", key: "k", mutualTLS: true}, "--mutual-tls requires --client-ca"},
		{"clientCA without TLS", cmdServerOptions{clientCA: "ca"}, "--client-ca requires --cert"},
		{"clientCA without mutualTLS", cmdServerOptions{cert: "c", key: "k", clientCA: "ca"}, "--client-ca requires --mutual-tls"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.opt.validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestStringOptions(t *testing.T) {
	for _, test := range []struct {
		name                string
		args                []string
		cfgFileContent      []byte
		clientCertStoreHit  string
		clientCertStoreMiss string
		clientKeyStoreHit   string
		clientKeyStoreMiss  string
		caCertStoreHit      string
		caCertStoreMiss     string
	}{
		{"Config with options set",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"client-cert": "/foo", "client-key": "/bar", "ca-cert": "/baz"}}}`),
			"/foo", "", "/bar", "", "/baz", "",
		},
		{"Configs set via command line args",
			[]string{"--client-cert", "/aa/bb", "--client-key", "/another", "--ca-cert", "/ca"},
			[]byte(`{"store-options": {"/store/*/":{"client-cert": "/foo", "client-key": "/bar", "ca-cert": "/baz"}}}`),
			"/aa/bb", "/aa/bb", "/another", "/another", "/ca", "/ca",
		},
		{"Config without any of those string options set",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"uncompressed": true}}}`),
			"", "", "", "", "", "",
		},
		{"Disable values from CLI args",
			[]string{"--client-cert", "", "--client-key", "", "--ca-cert", ""},
			[]byte(`{"store-options": {"/store/*/":{"client-cert": "/foo", "client-key": "/bar", "ca-cert": "/baz"}}}`),
			"", "", "", "", "", "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "desync-options")
			require.NoError(t, os.WriteFile(f, test.cfgFileContent, 0644))

			// Set the global config file name
			cfgFile = f

			initConfig()

			var cmdOpt cmdStoreOptions

			cmd := newTestOptionsCommand(&cmdOpt)
			cmd.SetArgs(test.args)

			// Execute the mock command, to load the options provided in the launch arguments
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			configOptions, err := cfg.GetStoreOptionsFor("/store/20230901")
			require.NoError(t, err)
			opt := cmdOpt.MergedWith(configOptions)
			require.Equal(t, test.clientCertStoreHit, opt.ClientCert)
			require.Equal(t, test.clientKeyStoreHit, opt.ClientKey)
			require.Equal(t, test.caCertStoreHit, opt.CACert)

			configOptions, err = cfg.GetStoreOptionsFor("/missingStore")
			opt = cmdOpt.MergedWith(configOptions)
			require.NoError(t, err)
			require.Equal(t, test.clientCertStoreMiss, opt.ClientCert)
			require.Equal(t, test.clientKeyStoreMiss, opt.ClientKey)
			require.Equal(t, test.caCertStoreMiss, opt.CACert)
		})
	}
}

func TestTrustInsecureOption(t *testing.T) {
	for _, test := range []struct {
		name                   string
		args                   []string
		cfgFileContent         []byte
		trustInsecureStoreHit  bool
		trustInsecureStoreMiss bool
	}{
		{"Config enables trust-insecure, no CLI flag",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"trust-insecure": true}}}`),
			true, false,
		},
		{"Config enables trust-insecure, CLI explicitly disables it",
			[]string{"--trust-insecure=false"},
			[]byte(`{"store-options": {"/store/*/":{"trust-insecure": true}}}`),
			false, false,
		},
		{"Config without trust-insecure, CLI enables it",
			[]string{"--trust-insecure"},
			[]byte(`{"store-options": {"/store/*/":{"uncompressed": true}}}`),
			true, true,
		},
		{"Config without trust-insecure, no CLI flag",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"uncompressed": true}}}`),
			false, false,
		},
		{"Config disables trust-insecure, CLI enables it",
			[]string{"--trust-insecure"},
			[]byte(`{"store-options": {"/store/*/":{"trust-insecure": false}}}`),
			true, true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "desync-options")
			require.NoError(t, os.WriteFile(f, test.cfgFileContent, 0644))

			// Set the global config file name
			cfgFile = f

			initConfig()

			var cmdOpt cmdStoreOptions

			cmd := newTestOptionsCommand(&cmdOpt)
			cmd.SetArgs(test.args)

			// Execute the mock command, to load the options provided in the launch arguments
			_, err := cmd.ExecuteC()
			require.NoError(t, err)

			configOptions, err := cfg.GetStoreOptionsFor("/store/20230901")
			require.NoError(t, err)
			opt := cmdOpt.MergedWith(configOptions)
			require.Equal(t, test.trustInsecureStoreHit, opt.TrustInsecure)

			configOptions, err = cfg.GetStoreOptionsFor("/missingStore")
			require.NoError(t, err)
			opt = cmdOpt.MergedWith(configOptions)
			require.Equal(t, test.trustInsecureStoreMiss, opt.TrustInsecure)
		})
	}
}
