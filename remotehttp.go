package desync

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"crypto/x509"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// TrustInsecure determines if invalid certs presented by HTTP stores should
// be accepted.
var TrustInsecure bool

// RemoteHTTP is a remote casync store accessed via HTTP.
type RemoteHTTP struct {
	location   *url.URL
	client     *fasthttp.Client
	errorRetry int
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

	var client *fasthttp.Client

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
		client = &fasthttp.Client{
			MaxConnsPerHost: n,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: TrustInsecure,
				Certificates:       []tls.Certificate{certificate},
				RootCAs:            caCertPool,
			},
		}
	} else {
		// Build a client with the right size connection pool and optionally disable
		// certificate verification.
		client = &fasthttp.Client{
			MaxIdleConnDuration: 2 * time.Second,
			MaxConnsPerHost:     n,
			TLSConfig:           &tls.Config{InsecureSkipVerify: TrustInsecure},
		}
	}

	// client := &http.Client{Transport: tr}

	return &RemoteHTTP{location: &u, client: client}, nil
}

// SetTimeout configures the timeout on the HTTP client for all requests
func (r *RemoteHTTP) SetTimeout(timeout time.Duration) {
	r.client.ReadTimeout = timeout
	r.client.WriteTimeout = timeout
	r.client.Dial = dialFunc(timeout)
}

// SetErrorRetry defines how many HTTP errors are retried. This can be useful
// when dealing with unreliable networks that can timeout or where errors are
// transient.
func (r *RemoteHTTP) SetErrorRetry(n int) {
	r.errorRetry = n
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (r *RemoteHTTP) GetChunk(id ChunkID) ([]byte, error) {
	sID := id.String()
	p := filepath.Join(sID[0:4], sID) + chunkFileExt

	u, _ := r.location.Parse(p)
	var (
		status  int
		err     error
		attempt int
		b       []byte
	)
	for {
		attempt++
		status, b, err = r.client.Get(nil, u.String())
		if err != nil {
			if attempt >= r.errorRetry {
				return nil, errors.Wrap(err, u.String())
			}
			continue
		}
		switch status {
		case fasthttp.StatusOK: // expected
		case fasthttp.StatusNotFound:
			return nil, ChunkMissing{id}
		default:
			return nil, fmt.Errorf("unexpected status code %d from %s", status, p)
		}
		break
	}
	return b, err
}

// HasChunk returns true if the chunk is in the store
func (r *RemoteHTTP) HasChunk(id ChunkID) bool {
	sID := id.String()
	p := filepath.Join(sID[0:4], sID) + chunkFileExt

	u, _ := r.location.Parse(p)
	var (
		req     = fasthttp.AcquireRequest()
		resp    = fasthttp.AcquireResponse()
		err     error
		attempt int
	)
	req.SetRequestURI(u.String())
	req.Header.SetMethod("HEAD")
retry:
	attempt++
	if err = r.client.Do(req, resp); err != nil {
		if attempt >= r.errorRetry {
			return false
		}
		goto retry
	}
	switch resp.StatusCode() {
	case fasthttp.StatusOK:
		return true
	default:
		return false
	}
}

// StoreChunk adds a new chunk to the store
func (r *RemoteHTTP) StoreChunk(id ChunkID, b []byte) error {
	sID := id.String()
	p := filepath.Join(sID[0:4], sID) + chunkFileExt

	u, _ := r.location.Parse(p)
	var (
		err     error
		attempt int
		req     = fasthttp.AcquireRequest()
		resp    = fasthttp.AcquireResponse()
	)
	req.SetRequestURI(u.String())
	req.Header.SetMethod("PUT")
	req.SetBody(b)
retry:
	attempt++
	if err = r.client.Do(req, resp); err != nil {
		if attempt >= r.errorRetry {
			return err
		}
		goto retry
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		return errors.New(string(resp.Body()))
	}
	return nil
}

func (r *RemoteHTTP) String() string {
	return r.location.String()
}

func (s RemoteHTTP) Close() error { return nil }

func dialFunc(timeout time.Duration) fasthttp.DialFunc {
	return func(addr string) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	}
}
