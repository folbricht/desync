package main

import (
	"os"
	"time"

	"path/filepath"

	"github.com/go-ini/ini"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
)

// SharedCredentialsFilename returns the SDK's default file path
// for the shared credentials file.
//
// Builds the shared config file path based on the OS's platform.
//
//   - Linux/Unix: $HOME/.aws/credentials
// 	 - Windows %USERPROFILE%\.aws\credentials
func SharedCredentialsFilename() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, ".aws", "credentials"), nil
}

// StaticCredentialsProvider implements credentials.Provider from github.com/minio/minio-go/pkg/credentials
type StaticCredentialsProvider struct {
	creds credentials.Value
}

// IsExpired returns true when the credentials are expired
func (cp *StaticCredentialsProvider) IsExpired() bool {
	return false
}

// Retrieve returns credentials
func (cp *StaticCredentialsProvider) Retrieve() (credentials.Value, error) {
	return cp.creds, nil
}

// NewStaticCredentials initializes a new set of S3 credentials
func NewStaticCredentials(accessKey, secretKey string) *credentials.Credentials {
	p := &StaticCredentialsProvider{
		credentials.Value{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	}
	return credentials.New(p)
}

// RefreshableSharedCredentialsProvider retrieves credentials from the current user's home
// directory, and keeps track if those credentials are expired.
//
// Profile ini file example: $HOME/.aws/credentials
type RefreshableSharedCredentialsProvider struct {
	// Path to the shared credentials file.
	//
	// If empty will look for "AWS_SHARED_CREDENTIALS_FILE" env variable. If the
	// env value is empty will default to current user's home directory.
	// Linux/OSX: "$HOME/.aws/credentials"
	Filename string

	// AWS Profile to extract credentials from the shared credentials file. If empty
	// will default to environment variable "AWS_PROFILE" or "default" if
	// environment variable is also not set.
	Profile string

	// The expiration time of the current fetched credentials.
	exp time.Time

	// The function to get the current timestamp
	now func() time.Time
}

// NewRefreshableSharedCredentials returns a pointer to a new Credentials object
// wrapping the Profile file provider.
func NewRefreshableSharedCredentials(filename string, profile string, now func() time.Time) *credentials.Credentials {
	return credentials.New(&RefreshableSharedCredentialsProvider{
		Filename: filename,
		Profile:  profile,

		// To ensure the credentials are always valid, the provider should fetch the credentials every 5 minutes or so.
		// It's set to 1 minute here.
		exp: now().Add(time.Minute),
		now: now,
	})
}

// IsExpired returns if the shared credentials have expired.
func (p *RefreshableSharedCredentialsProvider) IsExpired() bool {
	return p.now().After(p.exp)
}

// Retrieve reads and extracts the shared credentials from the current
// users home directory.
func (p *RefreshableSharedCredentialsProvider) Retrieve() (credentials.Value, error) {
	filename, err := p.filename()
	if err != nil {
		return credentials.Value{}, err
	}

	creds, err := loadProfile(filename, p.profile())
	if err != nil {
		return credentials.Value{}, err
	}

	// After retrieving the credentials, reset the expiration time.
	p.exp = p.now().Add(time.Minute)
	return creds, nil
}

// loadProfiles loads from the file pointed to by shared credentials filename for profile.
// The credentials retrieved from the profile will be returned or error. Error will be
// returned if it fails to read from the file, or the data is invalid.
func loadProfile(filename, profile string) (credentials.Value, error) {
	config, err := ini.Load(filename)
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "failed to load shared credentials file")
	}
	iniProfile, err := config.GetSection(profile)
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "failed to get profile")
	}

	id, err := iniProfile.GetKey("aws_access_key_id")
	if err != nil {
		return credentials.Value{}, errors.Wrapf(err, "shared credentials %s in %s did not contain aws_access_key_id", profile, filename)
	}

	secret, err := iniProfile.GetKey("aws_secret_access_key")
	if err != nil {
		return credentials.Value{}, errors.Wrapf(err, "shared credentials %s in %s did not contain aws_secret_access_key", profile, filename)
	}

	// Default to empty string if not found
	token := iniProfile.Key("aws_session_token")

	return credentials.Value{
		AccessKeyID:     id.String(),
		SecretAccessKey: secret.String(),
		SessionToken:    token.String(),
	}, nil
}

// filename returns the filename to use to read AWS shared credentials.
//
// Will return an error if the user's home directory path cannot be found.
func (p *RefreshableSharedCredentialsProvider) filename() (string, error) {
	if len(p.Filename) != 0 {
		return p.Filename, nil
	}

	if p.Filename = os.Getenv("AWS_SHARED_CREDENTIALS_FILE"); len(p.Filename) != 0 {
		return p.Filename, nil
	}

	// SDK's default file path
	// - Linux/Unix: $HOME/.aws/credentials
	// - Windows %USERPROFILE%\.aws\credentials
	filename, err := SharedCredentialsFilename()
	if err != nil {
		return "", errors.Wrap(err, "user home directory not found")
	}
	p.Filename = filename
	return p.Filename, nil
}

// profile returns the AWS shared credentials profile.  If empty will read
// environment variable "AWS_PROFILE". If that is not set profile will
// return "default".
func (p *RefreshableSharedCredentialsProvider) profile() string {
	if p.Profile == "" {
		p.Profile = os.Getenv("AWS_PROFILE")
	}
	if p.Profile == "" {
		p.Profile = "default"
	}

	return p.Profile
}
