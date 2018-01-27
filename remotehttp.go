package desync

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// RemoteHTTP is a remote casync store accessed via HTTP.
type RemoteHTTP struct {
	location *url.URL
}

func NewRemoteHTTPStore(location *url.URL) (*RemoteHTTP, error) {
	if location.Scheme != "http" && location.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme %s, expected http or https", location.Scheme)
	}
	// Make sure we have a trailing / on the path
	u := *location
	if !strings.HasSuffix(u.Path, "/") {
		u.Path = u.Path + "/"
	}
	return &RemoteHTTP{&u}, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (r *RemoteHTTP) GetChunk(id ChunkID) ([]byte, error) {
	sID := id.String()
	p := filepath.Join(sID[0:4], sID) + chunkFileExt

	u, _ := r.location.Parse(p)
	resp, err := http.Get(u.String())
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
