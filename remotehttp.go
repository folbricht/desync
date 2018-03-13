package desync

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"crypto/x509"
)

// TrustInsecure determines if invalid certs presented by HTTP stores should
// be accepted.
var TrustInsecure bool

// RemoteHTTP is a remote casync store accessed via HTTP.
type RemoteHTTP struct {
	location *url.URL
	client   *http.Client
}

// NewRemoteHTTPStore initializes a new store that pulls chunks via HTTP(S) from
// a remote web server. n defines the size of idle connections allowed.
func NewRemoteHTTPStore(location *url.URL, n int, cert string, key string) (*RemoteHTTP, error) {
	if location.Scheme != "http" && location.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme %s, expected http or https", location.Scheme)
	}
	// Make sure we have a trailing / on the path
	u := *location
	if !strings.HasSuffix(u.Path, "/") {
		u.Path = u.Path + "/"
	}

	var tr *http.Transport

	if cert != "" && key != "" {
		// Load client cert
		certificate, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate from %s", cert)
		}
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to create CaCertPool")
		}
		tr = &http.Transport{
			DisableCompression:  true,
			MaxIdleConnsPerHost: n,
			TLSClientConfig:     &tls.Config{
				InsecureSkipVerify: TrustInsecure,
				Certificates: []tls.Certificate{certificate},
				RootCAs:      caCertPool,
				},
		}

	} else {
		// Build a client with the right size connection pool and optionally disable
		// certificate verification.
		tr = &http.Transport{
			DisableCompression:  true,
			MaxIdleConnsPerHost: n,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: TrustInsecure},
		}
	}

	client := &http.Client{Transport: tr}

	return &RemoteHTTP{&u, client}, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (r *RemoteHTTP) GetChunk(id ChunkID) ([]byte, error) {
	sID := id.String()
	p := filepath.Join(sID[0:4], sID) + chunkFileExt

	u, _ := r.location.Parse(p)
	resp, err := r.client.Get(u.String())
	if err != nil {
		return nil, errors.Wrap(err, u.String())
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200: // expected
	case 404:
		return nil, ChunkMissing{id}
	default:
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, p)
	}
	return ioutil.ReadAll(resp.Body)
}

func (r *RemoteHTTP) String() string {
	return r.location.String()
}

func (s RemoteHTTP) Close() error { return nil }
