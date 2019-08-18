package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/folbricht/desync"
	"github.com/minio/minio-go/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// S3Creds holds credentials or references to an S3 credentials file.
type S3Creds struct {
	AccessKey          string `json:"access-key,omitempty"`
	SecretKey          string `json:"secret-key,omitempty"`
	AwsCredentialsFile string `json:"aws-credentials-file,omitempty"`
	AwsProfile         string `json:"aws-profile,omitempty"`
	// Having an explicit aws region makes minio slightly faster because it avoids url parsing
	AwsRegion string `json:"aws-region,omitempty"`
}

// Config is used to hold the global tool configuration. It's used to customize
// store features and provide credentials where needed.
type Config struct {
	HTTPTimeout    time.Duration                  `json:"http-timeout,omitempty"`
	HTTPErrorRetry int                            `json:"http-error-retry,omitempty"`
	S3Credentials  map[string]S3Creds             `json:"s3-credentials"`
	StoreOptions   map[string]desync.StoreOptions `json:"store-options"`
}

// GetS3CredentialsFor attempts to find creds and region for an S3 location in the
// config and the environment (which takes precedence). Returns a minio credentials
// struct and region string. If not found, the creds struct will return "" when invoked.
// Uses the scheme, host and port which need to match what's in the config file.
func (c Config) GetS3CredentialsFor(u *url.URL) (*credentials.Credentials, string) {
	// See if creds are defined in the ENV, if so, they take precedence
	accessKey := os.Getenv("S3_ACCESS_KEY")
	region := os.Getenv("S3_REGION")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if accessKey != "" || secretKey != "" {
		return NewStaticCredentials(accessKey, secretKey), region
	}

	// Look in the config to find a match for scheme+host
	key := &url.URL{
		Scheme: strings.TrimPrefix(u.Scheme, "s3+"),
		Host:   u.Host,
	}
	credsConfig := c.S3Credentials[key.String()]
	creds := NewStaticCredentials("", "")
	region = credsConfig.AwsRegion

	// if access access-key is present, it takes precedence
	if credsConfig.AccessKey != "" {
		creds = NewStaticCredentials(credsConfig.AccessKey, credsConfig.SecretKey)
	} else if credsConfig.AwsCredentialsFile != "" {
		creds = NewRefreshableSharedCredentials(credsConfig.AwsCredentialsFile, credsConfig.AwsProfile, time.Now)
	}
	return creds, region
}

// GetStoreOptionsFor returns optional config options for a specific store. Note that
// the location string in the config file needs to match exactly (watch for trailing /).
func (c Config) GetStoreOptionsFor(location string) desync.StoreOptions {
	for k, v := range c.StoreOptions {
		if locationMatch(k, location) {
			return v
		}
	}
	return desync.StoreOptions{}
}

func newConfigCommand(ctx context.Context) *cobra.Command {
	var write bool

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show or write config file",
		Long: `Shows the current internal configuration settings, either the defaults,
the values from $HOME/.config/desync/config.json or the specified config file. The
output can be used to create a custom config file writing it to the specified file
or $HOME/.config/desync/config.json by default.`,
		Example: `  desync config
  desync --config desync.json config -w`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConfig(ctx, write)
		},
		SilenceUsage: true,
	}

	flags := cmd.Flags()
	flags.BoolVarP(&write, "write", "w", false, "write current configuration to file")
	return cmd
}

func runConfig(ctx context.Context, write bool) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	var w io.Writer = os.Stderr
	if write {
		if err = os.MkdirAll(filepath.Dir(cfgFile), 0755); err != nil {
			return err
		}
		f, err := os.OpenFile(cfgFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Println("Writing config to", cfgFile)
		w = f
	}
	_, err = w.Write(b)
	fmt.Println()
	return err
}

// Global config in the main packe defining the defaults. Those can be
// overridden by loading a config file or in the command line.
var cfg Config
var cfgFile string

// Look for $HOME/.config/desync and if present, load into the global config
// instance. Values defined in the file will be set accordingly, while anything
// that's not in the file will retain it's default values.
func initConfig() {
	var defaultLocation bool
	if cfgFile == "" {
		switch runtime.GOOS {
		case "windows":
			cfgFile = filepath.Join(os.Getenv("HOMEDRIVE")+os.Getenv("HOMEPATH"), ".config", "desync", "config.json")
		default:
			cfgFile = filepath.Join(os.Getenv("HOME"), ".config", "desync", "config.json")
		}
		defaultLocation = true
	}
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		if defaultLocation { // no problem if the default config doesn't exist
			return
		}
		die(err)
	}
	f, err := os.Open(cfgFile)
	if err != nil {
		die(err)
	}
	defer f.Close()
	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		die(errors.Wrap(err, "reading "+cfgFile))
	}
}

// Digest algorithm to be used by desync globally.
var digestAlgorithm string

func setDigestAlgorithm() {
	switch digestAlgorithm {
	case "", "sha512-256":
		desync.Digest = desync.SHA512256{}
	case "sha256":
		desync.Digest = desync.SHA256{}
	default:
		die(fmt.Errorf("invalid digest algorithm '%s'", digestAlgorithm))
	}
}
