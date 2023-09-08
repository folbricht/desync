package main

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const defaultErrorRetry = 3

func newTestOptionsCommand(opt *cmdStoreOptions) *cobra.Command {
	cmd := &cobra.Command{}

	addStoreOptions(opt, cmd.Flags())
	return cmd
}

func TestErrorRetryOptions(t *testing.T) {
	for _, test := range []struct {
		name                string
		args                []string
		cfgFileContent      []byte
		errorRetryStoreHit  int
		errorRetryStoreMiss int
	}{
		{"Config with error-retry set",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20}}}`),
			20, defaultErrorRetry,
		},
		{"Error retry via command line args",
			[]string{"--error-retry", "10"},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20}}}`),
			10, 10,
		},
		{"Config without error-retry",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"uncompressed": true}}}`),
			defaultErrorRetry, defaultErrorRetry,
		},
		{"Config with default error-retry",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 3}}}`),
			defaultErrorRetry, defaultErrorRetry,
		},
		{"Config that disables error-retry",
			[]string{""},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 0}}}`),
			0, defaultErrorRetry,
		},
		{"Disables error-retry via command line args",
			[]string{"--error-retry", "0"},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20}}}`),
			0, 0,
		},
		{"Force the default values via command line args",
			[]string{"--error-retry", "3"},
			[]byte(`{"store-options": {"/store/*/":{"error-retry": 20}}}`),
			defaultErrorRetry, defaultErrorRetry,
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

			configOptions, err = cfg.GetStoreOptionsFor("/missingStore")
			opt = cmdOpt.MergedWith(configOptions)
			require.NoError(t, err)
			require.Equal(t, test.errorRetryStoreMiss, opt.ErrorRetry)
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
