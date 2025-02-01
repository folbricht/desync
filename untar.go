package desync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"

	"golang.org/x/sync/errgroup"
)

// UnTar implements the untar command, decoding a catar file and writing the
// contained tree to a target directory.
func UnTar(ctx context.Context, r io.Reader, fs FilesystemWriter) error {
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
			err = fs.CreateDir(n)
		case NodeFile:
			err = fs.CreateFile(n)
		case NodeDevice:
			err = fs.CreateDevice(n)
		case NodeSymlink:
			err = fs.CreateSymlink(n)
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

// UnTarIndex takes an index file (of a chunked catar), re-assembles the catar
// and decodes it on-the-fly into the target directory 'dst'. Uses n gorountines
// to retrieve and decompress the chunks.
func UnTarIndex(ctx context.Context, fs FilesystemWriter, index Index, s Store, n int, pb ProgressBar) error {
	type requestJob struct {
		chunk IndexChunk    // requested chunk
		data  chan ([]byte) // channel for the (decompressed) chunk
	}
	var (
		req      = make(chan requestJob)
		assemble = make(chan chan []byte, n)
	)
	g, ctx := errgroup.WithContext(ctx)

	// Initialize and start progress bar if one was provided
	pb.SetTotal(len(index.Chunks))
	pb.Start()
	defer pb.Finish()

	// Use a pipe as input to untar and write the chunks into that (in the right
	// order of course)
	r, w := io.Pipe()

	// Workers - getting chunks from the store
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for r := range req {
				// Pull the chunk from the store
				chunk, err := s.GetChunk(r.chunk.ID)
				if err != nil {
					close(r.data)
					return err
				}
				b, err := chunk.Data()
				if err != nil {
					close(r.data)
					return err
				}
				// Might as well verify the chunk size while we're at it
				if r.chunk.Size != uint64(len(b)) {
					close(r.data)
					return fmt.Errorf("unexpected size for chunk %s", r.chunk.ID.String())
				}
				r.data <- b
				close(r.data)
			}
			return nil
		})
	}

	// Feeder - requesting chunks from the workers and handing a result data channel
	// to the assembler
	g.Go(func() error {
	loop:
		for _, c := range index.Chunks {
			data := make(chan []byte, 1)
			select {
			case <-ctx.Done():
				break loop
			case req <- requestJob{chunk: c, data: data}: // request the chunk
				select {
				case <-ctx.Done():
					break loop
				case assemble <- data: // and hand over the data channel to the assembler
				}
			}
		}
		close(req)      // tell the workers this is it
		close(assemble) // tell the assembler we're done
		return nil
	})

	// Assember - Read from data channels push the chunks into the pipe that untar reads from
	g.Go(func() error {
		defer w.Close() // No more chunks to come, stop the untar
	loop:
		for {
			select {
			case data := <-assemble:
				if data == nil {
					break loop
				}
				pb.Increment()
				b := <-data
				if _, err := io.Copy(w, bytes.NewReader(b)); err != nil {
					return err
				}
			case <-ctx.Done():
				break loop
			}
		}
		return nil
	})

	// UnTar - Read from the pipe that Assembler pushes into
	g.Go(func() error {
		err := UnTar(ctx, r, fs)
		if err != nil {
			// If an error has occurred during the UnTar, we need to stop the Assembler.
			// If we don't, then it would stall on writing to the pipe.
			r.CloseWithError(err)
		}
		return err
	})

	return g.Wait()
}
