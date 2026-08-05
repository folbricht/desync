package main

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigFile(t *testing.T) {
	cfgFileContent := []byte(`{"store-options": {"/path/to/store/":{"uncompressed": true}}}`)
	f := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(f, cfgFileContent, 0644))

	// Set the global config file name
	cfgFile = f

	// Call init, this should use the custom config file and global "cfg" should contain the
	// values
	initConfig()

	// If everything worked, the options should be set according to the config file created above
	opt, err := cfg.GetStoreOptionsFor("/path/to/store")
	require.NoError(t, err)
	require.True(t, opt.Uncompressed)

	// The options for a non-matching store should be default
	opt, err = cfg.GetStoreOptionsFor("/path/other-store")
	require.NoError(t, err)
	require.False(t, opt.Uncompressed)
}

func TestConfigFileMultipleMatches(t *testing.T) {
	cfgFileContent := []byte(`{"store-options": {"/path/to/store/":{"uncompressed": true}, "/path/to/store":{"uncompressed": false}}}`)
	f := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(f, cfgFileContent, 0644))

	// Set the global config file name
	cfgFile = f

	// Call init, this should use the custom config file and global "cfg" should contain the
	// values
	initConfig()

	// We expect this to fail because both "/path/to/store/" and "/path/to/store" matches the
	// provided location
	_, err := cfg.GetStoreOptionsFor("/path/to/store")
	require.Error(t, err)
}

func TestGetOCICredentials(t *testing.T) {
	// Make sure credentials aren't picked up from the environment or the
	// user's Docker config
	t.Setenv("DESYNC_OCI_USERNAME", "")
	t.Setenv("DESYNC_OCI_PASSWORD", "")
	t.Setenv("DOCKER_CONFIG", t.TempDir())

	config := Config{OCICredentials: map[string]OCICreds{
		"oci+https://ghcr.io/user/repo":  {Username: "user", Secret: "secret"},
		"oci+https://registry.io/*/repo": {Username: "wildcard", Secret: "secret"},
		// A malformed glob pattern never matches but must not break the
		// lookup for other locations
		"oci+https://bad.example.com/[repo": {Username: "broken"},
	}}

	tests := map[string]struct {
		location string
		username string
	}{
		"exact match":          {"oci+https://ghcr.io/user/repo", "user"},
		"glob match":           {"oci+https://registry.io/anything/repo", "wildcard"},
		"trailing slash match": {"oci+https://ghcr.io/user/repo/", "user"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(test.location)
			require.NoError(t, err)
			credFunc, err := config.GetOCICredentialsFor(u)
			require.NoError(t, err)
			require.NotNil(t, credFunc)
			cred, err := credFunc(context.Background(), "")
			require.NoError(t, err)
			require.Equal(t, test.username, cred.Username)
		})
	}

	// A location without a match should fall back to the Docker credential
	// store without error
	u, err := url.Parse("oci+https://other.example.com/some/repo")
	require.NoError(t, err)
	_, err = config.GetOCICredentialsFor(u)
	require.NoError(t, err)

	// Multiple matching config entries are invalid
	multi := Config{OCICredentials: map[string]OCICreds{
		"oci+https://ghcr.io/user/repo": {Username: "a"},
		"oci+https://ghcr.io/*/repo":    {Username: "b"},
	}}
	u, err = url.Parse("oci+https://ghcr.io/user/repo")
	require.NoError(t, err)
	_, err = multi.GetOCICredentialsFor(u)
	require.Error(t, err)

	// A Docker config that exists but can't be parsed is an error, not a
	// silent fallback to anonymous access
	brokenDocker := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(brokenDocker, "config.json"), []byte("not json"), 0600))
	t.Setenv("DOCKER_CONFIG", brokenDocker)
	unmatched, err := url.Parse("oci+https://other.example.com/some/repo")
	require.NoError(t, err)
	_, err = config.GetOCICredentialsFor(unmatched)
	require.Error(t, err)
	t.Setenv("DOCKER_CONFIG", t.TempDir())

	// Credentials in the environment take precedence over the config
	t.Setenv("DESYNC_OCI_USERNAME", "envuser")
	t.Setenv("DESYNC_OCI_PASSWORD", "envpass")
	credFunc, err := config.GetOCICredentialsFor(u)
	require.NoError(t, err)
	cred, err := credFunc(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, "envuser", cred.Username)
	require.Equal(t, "envpass", cred.Password)
}

// Registries that take a token as the password accept it with any username,
// which makes setting only DESYNC_OCI_PASSWORD an easy mistake. It has to be
// reported here, or the variable is ignored and the registry answers with a
// 401 that points at nothing.
func TestGetOCICredentialsPasswordWithoutUsername(t *testing.T) {
	t.Setenv("DESYNC_OCI_USERNAME", "")
	t.Setenv("DESYNC_OCI_PASSWORD", "token")

	u, err := url.Parse("oci+https://ghcr.io/user/repo")
	require.NoError(t, err)
	_, err = Config{}.GetOCICredentialsFor(u)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DESYNC_OCI_USERNAME")
}

// The Docker config path can only be resolved from DOCKER_CONFIG or a home
// directory, and containers running as an arbitrary uid, systemd units and
// cron jobs have neither. Public repositories need no credentials at all, so
// that has to degrade to anonymous access instead of failing the operation.
func TestGetOCICredentialsWithoutDockerConfigPath(t *testing.T) {
	t.Setenv("DESYNC_OCI_USERNAME", "")
	t.Setenv("DESYNC_OCI_PASSWORD", "")
	t.Setenv("DOCKER_CONFIG", "")
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")

	u, err := url.Parse("oci+https://ghcr.io/public/repo")
	require.NoError(t, err)
	credFunc, err := Config{}.GetOCICredentialsFor(u)
	require.NoError(t, err)
	require.NotNil(t, credFunc)
	cred, err := credFunc(context.Background(), "")
	require.NoError(t, err)
	require.Empty(t, cred.Username)
	require.Empty(t, cred.Password)
}
