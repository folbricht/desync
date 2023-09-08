package main

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
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
			f, err := os.CreateTemp("", "desync-options")
			require.NoError(t, err)
			defer os.Remove(f.Name())
			_, err = f.Write(test.cfgFileContent)
			require.NoError(t, err)

			// Set the global config file name
			cfgFile = f.Name()

			initConfig()

			var cmdOpt cmdStoreOptions

			cmd := newTestOptionsCommand(&cmdOpt)
			cmd.SetArgs(test.args)

			// Execute the mock command, to load the options provided in the launch arguments
			_, err = cmd.ExecuteC()
			require.NoError(t, err)

			configOptions, err := cfg.GetStoreOptionsFor("/store/20230901")
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
			f, err := os.CreateTemp("", "desync-options")
			require.NoError(t, err)
			defer os.Remove(f.Name())
			_, err = f.Write(test.cfgFileContent)
			require.NoError(t, err)

			// Set the global config file name
			cfgFile = f.Name()

			initConfig()

			var cmdOpt cmdStoreOptions

			cmd := newTestOptionsCommand(&cmdOpt)
			cmd.SetArgs(test.args)

			// Execute the mock command, to load the options provided in the launch arguments
			_, err = cmd.ExecuteC()
			require.NoError(t, err)

			configOptions, err := cfg.GetStoreOptionsFor("/store/20230901")
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
