package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var now = time.Now

func TestNewRefreshableSharedCredentials(t *testing.T) {
	currentTime := time.Now()
	mockNow := func() time.Time {
		return currentTime.Add(time.Minute * 2)
	}

	c := NewRefreshableSharedCredentials("testdata/example.ini", "", mockNow)

	assert.True(t, c.IsExpired(), "Expect creds to be expired before retrieve")

	_, err := c.Get()
	assert.Nil(t, err, "Expect no error")

	assert.False(t, c.IsExpired(), "Expect creds to not be expired after retrieve")
}

func TestRefreshableSharedCredentialsProvider(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()

	p := RefreshableSharedCredentialsProvider{Filename: "testdata/example.ini", Profile: "", exp: now().Add(time.Minute), now: now}
	creds, err := p.Retrieve()
	assert.Nil(t, err, "Expect no error")

	assert.Equal(t, "accessKey", creds.AccessKeyID, "Expect access key ID to match")
	assert.Equal(t, "secret", creds.SecretAccessKey, "Expect secret access key to match")
	assert.Equal(t, "token", creds.SessionToken, "Expect session token to match")
}

func TestRefreshableSharedCredentialsProviderIsExpired(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()
	currentTime := time.Now()
	mockNow := func() time.Time {
		return currentTime.Add(time.Minute * 2)
	}

	p := RefreshableSharedCredentialsProvider{Filename: "testdata/example.ini", Profile: "", exp: currentTime.Add(time.Minute), now: mockNow}

	assert.True(t, p.IsExpired(), "Expect creds to be expired before retrieve")

	_, err := p.Retrieve()
	assert.Nil(t, err, "Expect no error")

	assert.False(t, p.IsExpired(), "Expect creds to not be expired after retrieve")
}

func TestRefreshableSharedCredentialsProviderWithAWS_SHARED_CREDENTIALS_FILE(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", "testdata/example.ini")

	p := RefreshableSharedCredentialsProvider{exp: now().Add(time.Minute), now: now}
	creds, err := p.Retrieve()

	assert.Nil(t, err, "Expect no error")

	assert.Equal(t, "accessKey", creds.AccessKeyID, "Expect access key ID to match")
	assert.Equal(t, "secret", creds.SecretAccessKey, "Expect secret access key to match")
	assert.Equal(t, "token", creds.SessionToken, "Expect session token to match")
}

func TestRefreshableSharedCredentialsProviderWithAWS_SHARED_CREDENTIALS_FILEAbsPath(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()

	wd, err := os.Getwd()
	assert.NoError(t, err)
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(wd, "testdata/example.ini"))
	p := RefreshableSharedCredentialsProvider{exp: now().Add(time.Minute), now: now}
	creds, err := p.Retrieve()
	assert.Nil(t, err, "Expect no error")

	assert.Equal(t, "accessKey", creds.AccessKeyID, "Expect access key ID to match")
	assert.Equal(t, "secret", creds.SecretAccessKey, "Expect secret access key to match")
	assert.Equal(t, "token", creds.SessionToken, "Expect session token to match")
}

func TestRefreshableSharedCredentialsProviderWithAWS_PROFILE(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()
	os.Setenv("AWS_PROFILE", "no_token")

	p := RefreshableSharedCredentialsProvider{Filename: "testdata/example.ini", Profile: "", exp: now().Add(time.Minute), now: now}
	creds, err := p.Retrieve()
	assert.Nil(t, err, "Expect no error")

	assert.Equal(t, "accessKey", creds.AccessKeyID, "Expect access key ID to match")
	assert.Equal(t, "secret", creds.SecretAccessKey, "Expect secret access key to match")
	assert.Empty(t, creds.SessionToken, "Expect no token")
}

func TestRefreshableSharedCredentialsProviderWithoutTokenFromProfile(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()

	p := RefreshableSharedCredentialsProvider{Filename: "testdata/example.ini", Profile: "no_token", exp: now().Add(time.Minute), now: now}
	creds, err := p.Retrieve()
	assert.Nil(t, err, "Expect no error")

	assert.Equal(t, "accessKey", creds.AccessKeyID, "Expect access key ID to match")
	assert.Equal(t, "secret", creds.SecretAccessKey, "Expect secret access key to match")
	assert.Empty(t, creds.SessionToken, "Expect no token")
}

func TestRefreshableSharedCredentialsProviderColonInCredFile(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()

	p := RefreshableSharedCredentialsProvider{Filename: "testdata/example.ini", Profile: "with_colon", exp: now().Add(time.Minute), now: now}
	creds, err := p.Retrieve()
	assert.Nil(t, err, "Expect no error")

	assert.Equal(t, "accessKey", creds.AccessKeyID, "Expect access key ID to match")
	assert.Equal(t, "secret", creds.SecretAccessKey, "Expect secret access key to match")
	assert.Empty(t, creds.SessionToken, "Expect no token")
}

func TestRefreshableSharedCredentialsProvider_DefaultFilename(t *testing.T) {
	defer restoreEnv(os.Environ())
	os.Clearenv()
	os.Setenv("USERPROFILE", "profile_dir")
	os.Setenv("HOME", "home_dir")

	// default filename and profile
	p := RefreshableSharedCredentialsProvider{exp: now().Add(time.Minute), now: now}

	filename, err := p.filename()

	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	if e, a := SharedCredentialsFilename(), filename; e != a {
		t.Errorf("expect %q filename, got %q", e, a)
	}
}

func restoreEnv(env []string) {
	for _, e := range env {
		kv := strings.SplitN(e, "=", 2)
		os.Setenv(kv[0], kv[1])
	}
}
