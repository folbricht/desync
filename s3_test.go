package desync

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"golang.org/x/sync/errgroup"
)

type MockCredProvider struct {
}

func (p *MockCredProvider) Retrieve() (credentials.Value, error) {
	return credentials.Value{
		AccessKeyID:     "mainone",
		SecretAccessKey: "thisiskeytrustmedude",
		SessionToken:    "youdontneedtoken",
		SignerType:      credentials.SignatureDefault,
	}, nil
}

func (p *MockCredProvider) IsExpired() bool {
	return false
}

func response(request *http.Request, headers http.Header, statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		ProtoMajor: 1,
		ProtoMinor: 0,
		Request:    request,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     headers,
	}
}

func sendObject(t *testing.T, conn *net.TCPConn, request *http.Request, filePath string, sendRst bool) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			resp := response(request, http.Header{}, 404, "")
			resp.Write(conn)
		} else {
			resp := response(request, http.Header{}, 500, err.Error())
			resp.Write(conn)
		}
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		resp := response(request, http.Header{}, 500, err.Error())
		resp.Write(conn)
		return
	}
	headers := http.Header{}
	headers.Add("Last-Modified", stat.ModTime().Format(http.TimeFormat))
	headers.Add("Content-Type", "application/octet-stream")
	headers.Add("Content-Length", strconv.FormatInt(stat.Size(), 10))

	if !sendRst {
		resp := http.Response{
			StatusCode: 200,
			ProtoMajor: 1,
			ProtoMinor: 0,
			Request:    request,
			Body:       file,
			Header:     headers,
		}
		resp.Write(conn)
	} else {
		if _, err := io.WriteString(conn, "HTTP/1.0 200 OK\r\n"); err != nil {
			t.Fatal(err)
		}
		if err := headers.Write(conn); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(conn, "\r\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := io.CopyN(conn, file, stat.Size()/2); err != nil {
			t.Fatal(err)
		}
		// it seems that setting SO_LINGER to 0 and calling close() on the socket forces server to
		// send RST TCP packet, which we will use to emulate network error
		if err := conn.SetLinger(0); err != nil {
			t.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func handleGetObjectRequest(t *testing.T, conn *net.TCPConn, bucket, store string, errorTimes *int, errorTimesLimit int) error {
	defer conn.Close()
	objectGetMatcher := regexp.MustCompile(`^/` + bucket + `/(.+)$`)

	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		return err
	}

	matches := objectGetMatcher.FindStringSubmatch(request.URL.Path)
	if matches != nil {
		sendObject(t, conn, request, store+"/"+matches[1], *errorTimes < errorTimesLimit)
		(*errorTimes)++
	} else {
		resp := response(request, http.Header{}, 400, "")
		resp.Write(conn)
	}
	return nil
}

// Run S3 server that can respond objects from `store`
// if `errorTimesLimit` > 0 server will send RST packet `errorTimesLimit` times after sending half of file
func getTcpS3Server(t *testing.T, ctx context.Context, bucket, store string, errorTimesLimit int) (net.Listener, *errgroup.Group) {
	group := errgroup.Group{}
	var errorTimes int
	// using localhost + resolver let us work on hosts that support only ipv6 or only ipv4
	ip, err := net.DefaultResolver.LookupIP(ctx, "ip", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if len(ip) < 1 {
		t.Fatalf("cannot resolve localhost")
	}

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: ip[0], Port: 0})
	if err != nil {
		t.Fatal(err)
	}

	group.Go(func() error {
		<-ctx.Done()
		return listener.Close()
	})

	group.Go(func() error {
		for {
			conn, err := listener.AcceptTCP()

			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}
				return err
			}
			err = handleGetObjectRequest(t, conn, bucket, store, &errorTimes, errorTimesLimit)
			if err != nil {
				return err
			}
		}
	})
	return listener, &group
}

func TestS3StoreGetChunk(t *testing.T) {
	chunkId, err := ChunkIDFromString("dda036db05bc2b99b6b9303d28496000c34b246457ae4bbf00fe625b5cabd7cd")
	if err != nil {
		t.Fatal(err)
	}
	location := "vertucon-central"
	bucket := "doomsdaydevices"
	provider := MockCredProvider{}

	t.Run("no_error", func(t *testing.T) {
		// Just try to get chunk from well-behaved S3 server, no errors expected
		ctx, cancel := context.WithCancel(context.Background())

		ln, group := getTcpS3Server(t, ctx, bucket, "cmd/desync/testdata", 0)

		endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
		store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
		if err != nil {
			t.Fatal(err)
		}

		chunk, err := store.GetChunk(chunkId)
		if err != nil {
			t.Fatal(err)
		}
		if chunk.ID() != chunkId {
			t.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
		}

		cancel()
		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fail", func(t *testing.T) {
		// Emulate network error - after sending half of the file S3 server sends RST to the client
		// We don't have retries here so we expect that GetChunk() will return error
		ctx, cancel := context.WithCancel(context.Background())

		ln, group := getTcpS3Server(t, ctx, bucket, "cmd/desync/testdata", 1)

		endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
		store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
		if err != nil {
			t.Fatal(err)
		}

		_, err = store.GetChunk(chunkId)
		opError := &net.OpError{}
		if err == nil || !errors.As(err, &opError) {
			t.Fatal(err)
		}

		cancel()
		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("recover", func(t *testing.T) {
		// Emulate network error - after sending half of the file S3 server sends RST to the client
		// We have retries here so we expect that GetChunk() will not return error
		ctx, cancel := context.WithCancel(context.Background())

		ln, group := getTcpS3Server(t, ctx, bucket, "cmd/desync/testdata", 1)

		endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
		store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{ErrorRetry: 1}, minio.BucketLookupAuto)
		if err != nil {
			t.Fatal(err)
		}

		chunk, err := store.GetChunk(chunkId)
		if err != nil {
			t.Fatal(err)
		}
		if chunk.ID() != chunkId {
			t.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
		}

		cancel()
		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})
}
