package desync

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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

// s3ErrMode describes how the fake S3 server should mishandle an object request.
type s3ErrMode int

const (
	s3ErrNone        s3ErrMode = iota // serve the object normally
	s3ErrRST                          // send half the body, then force a TCP RST (transport error)
	s3ErrCorruptBody                  // serve a complete, well-formed response whose body is truncated
)

func sendObject(conn *net.TCPConn, request *http.Request, filePath string, mode s3ErrMode) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			resp := response(request, http.Header{}, 404, "")
			resp.Write(conn)
		} else {
			resp := response(request, http.Header{}, 500, err.Error())
			resp.Write(conn)
		}
		return nil
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		resp := response(request, http.Header{}, 500, err.Error())
		resp.Write(conn)
		return nil
	}
	headers := http.Header{}
	headers.Add("Last-Modified", stat.ModTime().Format(http.TimeFormat))
	headers.Add("Content-Type", "application/octet-stream")

	switch mode {
	case s3ErrRST:
		headers.Add("Content-Length", strconv.FormatInt(stat.Size(), 10))
		if _, err := io.WriteString(conn, "HTTP/1.0 200 OK\r\n"); err != nil {
			return err
		}
		if err := headers.Write(conn); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, "\r\n"); err != nil {
			return err
		}
		if _, err := io.CopyN(conn, file, stat.Size()/2); err != nil {
			return err
		}
		// it seems that setting SO_LINGER to 0 and calling close() on the socket forces server to
		// send RST TCP packet, which we will use to emulate network error
		if err := conn.SetLinger(0); err != nil {
			return err
		}
		if err := conn.Close(); err != nil {
			return err
		}
	case s3ErrCorruptBody:
		// Serve a complete, well-formed HTTP response whose Content-Length matches
		// the bytes actually written, but only write the first half of the file. The
		// client reads it cleanly (no transport error), but the truncated chunk data
		// fails to decompress/validate - the scenario reported in issue #334.
		half := stat.Size() / 2
		headers.Add("Content-Length", strconv.FormatInt(half, 10))
		if _, err := io.WriteString(conn, "HTTP/1.0 200 OK\r\n"); err != nil {
			return err
		}
		if err := headers.Write(conn); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, "\r\n"); err != nil {
			return err
		}
		if _, err := io.CopyN(conn, file, half); err != nil {
			return err
		}
	default:
		headers.Add("Content-Length", strconv.FormatInt(stat.Size(), 10))
		resp := http.Response{
			StatusCode: 200,
			ProtoMajor: 1,
			ProtoMinor: 0,
			Request:    request,
			Body:       file,
			Header:     headers,
		}
		resp.Write(conn)
	}
	return nil
}

func handleGetObjectRequest(conn *net.TCPConn, bucket, store string, errorMode s3ErrMode, errorTimes *int, errorTimesLimit int) error {
	defer conn.Close()
	objectGetMatcher := regexp.MustCompile(`^/` + bucket + `/(.+)$`)

	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		return err
	}

	matches := objectGetMatcher.FindStringSubmatch(request.URL.Path)
	if matches != nil {
		mode := s3ErrNone
		if *errorTimes < errorTimesLimit {
			mode = errorMode
		}
		err = sendObject(conn, request, store+"/"+matches[1], mode)
		(*errorTimes)++
	} else {
		resp := response(request, http.Header{}, 400, "")
		resp.Write(conn)
	}
	return err
}

// Run S3 server that can respond objects from `store`. The first `errorTimesLimit`
// object requests are mishandled according to `errorMode` (e.g. truncated body or
// forced TCP reset); subsequent requests are served normally.
func getTcpS3Server(t *testing.T, group *errgroup.Group, ctx context.Context, bucket, store string, errorMode s3ErrMode, errorTimesLimit int) net.Listener {
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
			err = handleGetObjectRequest(conn, bucket, store, errorMode, &errorTimes, errorTimesLimit)
			if err != nil {
				return err
			}
		}
	})
	return listener
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
		group, gCtx := errgroup.WithContext(ctx)

		ln := getTcpS3Server(t, group, ctx, bucket, "cmd/desync/testdata", s3ErrNone, 0)

		group.Go(func() error {
			defer cancel()
			endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
			store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
			if err != nil {
				return err
			}

			c := make(chan error)
			go func() {
				chunk, err := store.GetChunk(chunkId)
				if err != nil {
					c <- err
				}
				if chunk.ID() != chunkId {
					c <- fmt.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
				}
				c <- nil
			}()
			select {
			case <-gCtx.Done():
				return nil
			case err = <-c:
				return err
			}
		})

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fail", func(t *testing.T) {
		// Emulate network error - after sending half of the file S3 server sends RST to the client
		// We don't have retries here so we expect that GetChunk() will return error
		ctx, cancel := context.WithCancel(context.Background())
		group, gCtx := errgroup.WithContext(ctx)

		ln := getTcpS3Server(t, group, ctx, bucket, "cmd/desync/testdata", s3ErrRST, 1)

		group.Go(func() error {
			defer cancel()
			endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
			store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
			if err != nil {
				return err
			}

			c := make(chan error)
			go func() {
				_, err = store.GetChunk(chunkId)
				opError := &net.OpError{}
				if err == nil || !errors.As(err, &opError) {
					c <- err
				}
				c <- nil
			}()
			select {
			case <-gCtx.Done():
				return nil
			case err = <-c:
				return err
			}
		})

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("recover", func(t *testing.T) {
		// Emulate network error - after sending half of the file S3 server sends RST to the client
		// We have retries here so we expect that GetChunk() will not return error
		ctx, cancel := context.WithCancel(context.Background())
		group, gCtx := errgroup.WithContext(ctx)

		ln := getTcpS3Server(t, group, ctx, bucket, "cmd/desync/testdata", s3ErrRST, 1)

		group.Go(func() error {
			defer cancel()
			endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
			store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{ErrorRetry: 1}, minio.BucketLookupAuto)
			if err != nil {
				return err
			}

			c := make(chan error)
			go func() {
				chunk, err := store.GetChunk(chunkId)
				if err != nil {
					c <- err
				}
				if chunk.ID() != chunkId {
					c <- fmt.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
				}
				c <- nil
			}()
			select {
			case <-gCtx.Done():
				return nil
			case err = <-c:
				return err
			}
		})

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("corrupt_body_fail", func(t *testing.T) {
		// Server returns a complete, well-formed response with a truncated body, so
		// the chunk data fails to decompress/validate (issue #334). With no retries
		// configured GetChunk() must return that error - and it must carry the
		// underlying decode failure, not the bogus "hash 0000..." mismatch.
		ctx, cancel := context.WithCancel(context.Background())
		group, gCtx := errgroup.WithContext(ctx)

		ln := getTcpS3Server(t, group, ctx, bucket, "cmd/desync/testdata", s3ErrCorruptBody, 1)

		group.Go(func() error {
			defer cancel()
			endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
			store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{}, minio.BucketLookupAuto)
			if err != nil {
				return err
			}

			c := make(chan error)
			go func() {
				_, err := store.GetChunk(chunkId)
				if err == nil {
					c <- errors.New("expected GetChunk to fail on truncated chunk body")
					return
				}
				var ci ChunkInvalid
				if !errors.As(err, &ci) {
					c <- fmt.Errorf("expected ChunkInvalid, got %T: %v", err, err)
					return
				}
				if ci.Err == nil {
					c <- fmt.Errorf("expected ChunkInvalid to carry the underlying decode error, got %v", err)
					return
				}
				c <- nil
			}()
			select {
			case <-gCtx.Done():
				return nil
			case err = <-c:
				return err
			}
		})

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("corrupt_body_recover", func(t *testing.T) {
		// Same truncated-body scenario, but with retries enabled GetChunk() should
		// retry the validation failure and succeed once a full response is served.
		ctx, cancel := context.WithCancel(context.Background())
		group, gCtx := errgroup.WithContext(ctx)

		ln := getTcpS3Server(t, group, ctx, bucket, "cmd/desync/testdata", s3ErrCorruptBody, 1)

		group.Go(func() error {
			defer cancel()
			endpoint := url.URL{Scheme: "s3+http", Host: ln.Addr().String(), Path: "/" + bucket + "/blob1.store/"}
			store, err := NewS3Store(&endpoint, credentials.New(&provider), location, StoreOptions{ErrorRetry: 1}, minio.BucketLookupAuto)
			if err != nil {
				return err
			}

			c := make(chan error)
			go func() {
				chunk, err := store.GetChunk(chunkId)
				if err != nil {
					c <- err
					return
				}
				if chunk.ID() != chunkId {
					c <- fmt.Errorf("got chunk with id equal to %q, expected %q", chunk.ID(), chunkId)
					return
				}
				c <- nil
			}()
			select {
			case <-gCtx.Done():
				return nil
			case err = <-c:
				return err
			}
		})

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}
	})
}
