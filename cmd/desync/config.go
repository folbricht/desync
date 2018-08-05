package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/pkg/credentials"
	"github.com/pkg/errors"
)

type S3Creds struct {
	AccessKey          string `json:"access-key,omitempty"`
	SecretKey          string `json:"secret-key,omitempty"`
	AwsCredentialsFile string `json:"aws-credentials-file,omitempty"`
	AwsProfile         string `json:"aws-profile,omitempty"`
	// Having an explicit aws region makes minio slightly faster because it avoids url parsing
	AwsRegion string `json:"aws-region,omitempty"`
}

type Config struct {
	HTTPTimeout    time.Duration      `json:"http-timeout"`
	HTTPErrorRetry int                `json:"http-error-retry"`
	S3Credentials  map[string]S3Creds `json:"s3-credentials"`
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

// Global config in the main packe defining the defaults. Those can be
// overridden by loading a config file.
var cfg = Config{
	HTTPTimeout: time.Minute,
}

const configUsage = `desync config

Shows the current internal config settings, either the defaults or the values
from $HOME/.config/desync/config.json. The output can be used to create a custom
config file by writing it to $HOME/.config/desync/config.json.
`

func config(ctx context.Context, args []string) error {
	var writeConfig bool
	flags := flag.NewFlagSet("config", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, configUsage)
		flags.PrintDefaults()
	}
	flags.BoolVar(&writeConfig, "w", false, "write current configuration to $HOME/.config/desync/config.json")
	flags.Parse(args)

	if flags.NArg() > 0 {
		return errors.New("Too many arguments. See -h for help.")
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	var w io.Writer = os.Stderr
	if writeConfig {
		filename, err := configFile()
		if err != nil {
			return err
		}
		if err = os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
			return err
		}
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Println("Writing config to", filename)
		w = f
	}
	_, err = w.Write(b)
	fmt.Println()
	return err
}

func configFile() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	filename := filepath.Join(u.HomeDir, ".config", "desync", "config.json")
	return filename, nil
}

// Look for $HOME/.config/desync and if present, load into the global config
// instance. Values defined in the file will be set accordingly, while anything
// that's not in the file will retain it's default values.
func loadConfigIfPresent() error {
	filename, err := configFile()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(&cfg)
	return errors.Wrap(err, "reading "+filename)
}
