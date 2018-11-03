package desync

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"crypto/x509"

	"github.com/pkg/errors"
)

// TrustInsecure determines if invalid certs presented by HTTP stores should
// be accepted.
var TrustInsecure bool

// RemoteHTTPBase is the base object for a remote, HTTP-based chunk or index stores.
type RemoteHTTPBase struct {
	location *url.URL
	client   *http.Client
	opt      StoreOptions
}

// RemoteHTTP is a remote casync store accessed via HTTP.
type RemoteHTTP struct {
	*RemoteHTTPBase
}

// NewRemoteHTTPStoreBase initializes a base object for HTTP index or chunk stores.
func NewRemoteHTTPStoreBase(location *url.URL, opt StoreOptions) (*RemoteHTTPBase, error) {
	if location.Scheme != "http" && location.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme %s, expected http or https", location.Scheme)
	}
	// Make sure we have a trailing / on the path
	u := *location
	if !strings.HasSuffix(u.Path, "/") {
		u.Path = u.Path + "/"
	}

	var tr *http.Transport

	if opt.ClientCert != "" && opt.ClientKey != "" {
		// Load client cert
		certificate, err := tls.LoadX509KeyPair(opt.ClientCert, opt.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate from %s", opt.ClientCert)
		}
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to create CaCertPool")
		}
		tr = &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DisableCompression:  true,
			MaxIdleConnsPerHost: opt.N,
			IdleConnTimeout:     60 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: TrustInsecure,
				Certificates:       []tls.Certificate{certificate},
				RootCAs:            caCertPool,
			},
		}

	} else {
		// Build a client with the right size connection pool and optionally disable
		// certificate verification.
		tr = &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DisableCompression:  true,
			MaxIdleConnsPerHost: opt.N,
			IdleConnTimeout:     60 * time.Second,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: TrustInsecure},
		}
	}

	timeout := opt.Timeout
	if timeout == 0 {
		timeout = time.Minute
	}
	client := &http.Client{Transport: tr, Timeout: timeout}

	return &RemoteHTTPBase{location: location, client: client, opt: opt}, nil
}

func (r *RemoteHTTPBase) String() string {
	return r.location.String()
}

// Close the HTTP store. NOP operation but needed to implement the interface.
func (r *RemoteHTTPBase) Close() error { return nil }

// GetObject reads and returns an object in the form of []byte from the store
func (r *RemoteHTTPBase) GetObject(name string) ([]byte, error) {
	u, _ := r.location.Parse(name)
	var (
		resp    *http.Response
		err     error
		attempt int
		b       []byte
	)
	for {
		attempt++
		resp, err = r.client.Get(u.String())
		if err != nil {
			if attempt >= r.opt.ErrorRetry {
				return nil, errors.Wrap(err, u.String())
			}
			continue
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200: // expected
		case 404:
			return nil, NoSuchObject{name}
		default:
			return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, name)
		}
		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			if attempt >= r.opt.ErrorRetry {
				return nil, errors.Wrap(err, u.String())
			}
			continue
		}
		break
	}
	return b, err
}

// StoreObject stores an object to the store.
func (r *RemoteHTTPBase) StoreObject(name string, rdr io.Reader) error {

	u, _ := r.location.Parse(name)
	var (
		resp    *http.Response
		err     error
		attempt int
	)
retry:
	attempt++
	req, err := http.NewRequest("PUT", u.String(), rdr)
	if err != nil {
		return err
	}
	resp, err = r.client.Do(req)
	if err != nil {
		if attempt >= r.opt.ErrorRetry {
			return err
		}
		goto retry
	}
	defer resp.Body.Close()
	msg, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return errors.New(string(msg))
	}
	return nil
}

// NewRemoteHTTPStore initializes a new store that pulls chunks via HTTP(S) from
// a remote web server. n defines the size of idle connections allowed.
func NewRemoteHTTPStore(location *url.URL, opt StoreOptions) (*RemoteHTTP, error) {
	b, err := NewRemoteHTTPStoreBase(location, opt)
	if err != nil {
		return nil, err
	}
	return &RemoteHTTP{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (r *RemoteHTTP) GetChunk(id ChunkID) (*Chunk, error) {
	p := r.nameFromID(id)
	b, err := r.GetObject(p)
	if err != nil {
		return nil, err
	}
	if r.opt.Uncompressed {
		return NewChunkWithID(id, b, nil, r.opt.SkipVerify)
	}
	return NewChunkWithID(id, nil, b, r.opt.SkipVerify)
}

// HasChunk returns true if the chunk is in the store
func (r *RemoteHTTP) HasChunk(id ChunkID) bool {
	p := r.nameFromID(id)
	u, _ := r.location.Parse(p)
	var (
		resp    *http.Response
		err     error
		attempt int
	)
retry:
	attempt++
	resp, err = r.client.Head(u.String())
	if err != nil {
		if attempt >= r.opt.ErrorRetry {
			return false
		}
		goto retry
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		return true
	default:
		return false
	}
}

// StoreChunk adds a new chunk to the store
func (r *RemoteHTTP) StoreChunk(chunk *Chunk) error {
	p := r.nameFromID(chunk.ID())
	var (
		b   []byte
		err error
	)
	if r.opt.Uncompressed {
		b, err = chunk.Uncompressed()
	} else {
		b, err = chunk.Compressed()
	}
	if err != nil {
		return err
	}
	return r.StoreObject(p, bytes.NewReader(b))
}

func (r *RemoteHTTP) nameFromID(id ChunkID) string {
	sID := id.String()
	name := path.Join(sID[0:4], sID)
	if r.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}
