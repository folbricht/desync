// +build !windows

package desync

import (
	"bytes"
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
)

// UnTar implements the untar command, decoding a catar file and writing the
// contained tree to a target directory.
func UnTar(ctx context.Context, r io.Reader, dst string) error {
	dec := NewArchiveDecoder(r)
loop:
	for {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		c, err := dec.Next()
		if err != nil {
			return err
		}
		switch n := c.(type) {
		case NodeDirectory:
			err = makeDir(dst, n)
		case NodeFile:
			err = makeFile(dst, n)
		case NodeDevice:
			err = makeDevice(dst, n)
		case NodeSymlink:
			err = makeSymlink(dst, n)
		case nil:
			break loop
		default:
			err = fmt.Errorf("unsupported type %s", reflect.TypeOf(c))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func makeDir(base string, n NodeDirectory) error {
	dst := filepath.Join(base, n.Name)

	// Let's see if there is a dir with the same name already
	if info, err := os.Stat(dst); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", dst)
		}
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		if err := os.Mkdir(dst, n.Mode); err != nil {
			return err
		}
	}
	// The dir exists now, fix the UID/GID
	if err := os.Chown(dst, n.UID, n.GID); err != nil {
		return err
	}
	if err := os.Chtimes(dst, n.MTime, n.MTime); err != nil {
		return err
	}
	return syscall.Chmod(dst, uint32(n.Mode))
}

func makeFile(base string, n NodeFile) error {
	dst := filepath.Join(base, n.Name)

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(f, n.Data); err != nil {
		return err
	}
	if err = f.Chown(n.UID, n.GID); err != nil {
		return err
	}
	if err = os.Chtimes(dst, n.MTime, n.MTime); err != nil {
		return err
	}
	return syscall.Chmod(dst, uint32(n.Mode))
}

func makeSymlink(base string, n NodeSymlink) error {
	dst := filepath.Join(base, n.Name)

	if err := os.Symlink(n.Target, dst); err != nil {
		return err
	}
	// TODO: On Linux, the permissions of the link don't matter so we don't
	// set them here. But they do matter somewhat on Mac, so should probably
	// add some Mac-specific logic for that here.
	// fchmodat() with flag AT_SYMLINK_NOFOLLOW
	return os.Lchown(dst, n.UID, n.GID)
}

func makeDevice(base string, n NodeDevice) error {
	dst := filepath.Join(base, n.Name)

	if err := syscall.Mknod(dst, uint32(n.Mode), int(mkdev(n.Major, n.Minor))); err != nil {
		return err
	}
	if err := os.Chown(dst, n.UID, n.GID); err != nil {
		return err
	}
	if err := os.Chtimes(dst, n.MTime, n.MTime); err != nil {
		return err
	}
	return syscall.Chmod(dst, uint32(n.Mode))
}

func mkdev(major, minor uint64) uint64 {
	dev := (major & 0x00000fff) << 8
	dev |= (major & 0xfffff000) << 32
	dev |= (minor & 0x000000ff) << 0
	dev |= (minor & 0xffffff00) << 12
	return dev
}

// UnTarIndex takes an index file (of a chunked catar), re-assembles the catar
// and decodes it on-the-fly into the target directory 'dst'. Uses n gorountines
// to retrieve and decompress the chunks.
func UnTarIndex(ctx context.Context, dst string, index Index, s Store, n int) error {
	type requestJob struct {
		chunk IndexChunk    // requested chunk
		data  chan ([]byte) // channel for the (decompressed) chunk
	}
	var (
		// stop bool
		wg       sync.WaitGroup
		mu       sync.Mutex
		pErr     error
		req      = make(chan requestJob)
		assemble = make(chan chan []byte, n)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Helper function to record and deal with any errors in the goroutines
	recordError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if pErr == nil {
			pErr = err
		}
		cancel()
	}

	// Use a pipe as input to untar and write the chunks into that (in the right
	// order of course)
	r, w := io.Pipe()

	// Workers - getting chunks from the store
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for r := range req {
				// Pull the (compressed) chunk from the store
				b, err := s.GetChunk(r.chunk.ID)
				if err != nil {
					recordError(err)
					continue
				}
				// Since we know how big the chunk is supposed to be, pre-allocate a
				// slice to decompress into
				db := make([]byte, r.chunk.Size)
				// The the chunk is compressed. Decompress it here
				db, err = Decompress(db, b)
				if err != nil {
					recordError(err)
					continue
				}
				// Verify the checksum of the chunk matches the ID
				sum := sha512.Sum512_256(db)
				if sum != r.chunk.ID {
					recordError(fmt.Errorf("unexpected sha512/256 %s for chunk id %s", sum, r.chunk.ID))
					continue
				}
				// Might as well verify the chunk size while we're at it
				if r.chunk.Size != uint64(len(db)) {
					recordError(fmt.Errorf("unexpected size for chunk %s", r.chunk.ID))
					continue
				}
				r.data <- db
				close(r.data)
			}
			wg.Done()
		}()
	}

	// Feeder - requesting chunks from the workers
	go func() {
	loop:
		for _, c := range index.Chunks {
			// See if we're meant to stop
			select {
			case <-ctx.Done():
				break loop
			default:
			}
			data := make(chan []byte, 1)
			req <- requestJob{chunk: c, data: data} // request the chunk
			assemble <- data                        // and hand over the data channel to the assembler
		}
		close(req)
		wg.Wait()       // wait for the workers to stop
		close(assemble) // tell the assembler we're done
	}()

	// Assember - push the chunks into the pipe that untar reads from
	go func() {
		for data := range assemble {
			b := <-data
			if _, err := io.Copy(w, bytes.NewReader(b)); err != nil {
				recordError(err)
			}
		}
		w.Close() // No more chunks to come, stop the untar
	}()

	// Run untar in the main goroutine
	if err := UnTar(ctx, r, dst); err != nil {
		return err
	}
	return pErr
}
