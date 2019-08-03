package desync

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestHTTPStoreURL(t *testing.T) {
	var requestURI string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestURI = r.RequestURI
	}))
	defer ts.Close()
	u, _ := url.Parse(ts.URL)

	chunkID := ChunkID{1, 2, 3, 4}
	tests := map[string]struct {
		storePath  string
		serverPath string
	}{
		"no path":             {"", "/0102/0102030400000000000000000000000000000000000000000000000000000000.cacnk"},
		"slash only":          {"/", "/0102/0102030400000000000000000000000000000000000000000000000000000000.cacnk"},
		"no trailing slash":   {"/path", "/path/0102/0102030400000000000000000000000000000000000000000000000000000000.cacnk"},
		"with trailing slash": {"/path/", "/path/0102/0102030400000000000000000000000000000000000000000000000000000000.cacnk"},
		"long path":           {"/path1/path2", "/path1/path2/0102/0102030400000000000000000000000000000000000000000000000000000000.cacnk"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			u.Path = test.storePath
			s, err := NewRemoteHTTPStore(u, StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			s.GetChunk(chunkID)
			if requestURI != test.serverPath {
				t.Fatalf("got request uri '%s', want '%s'", requestURI, test.serverPath)
			}
		})
	}
}
