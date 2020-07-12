package desync

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type benchmark struct {
	numReads   uint64
	seek       int64
	read       int64
	decompress int64
	diskRead   int64
}

var bench = &benchmark{}

func (b *benchmark) addSeek(since time.Time) {
	delta := int64(time.Since(since))
	atomic.AddInt64(&b.seek, delta)
}

func (b *benchmark) addRead(since time.Time) {
	delta := int64(time.Since(since))
	atomic.AddInt64(&b.read, delta)
}

func (b *benchmark) addDecompress(since time.Time) {
	delta := int64(time.Since(since))
	atomic.AddInt64(&b.decompress, delta)
}

func (b *benchmark) addDiskRead(since time.Time) {
	delta := int64(time.Since(since))
	atomic.AddInt64(&b.diskRead, delta)
}

func (b *benchmark) incReads() {
	atomic.AddUint64(&b.numReads, 1)
}

func (b *benchmark) String() string {
	seek := atomic.LoadInt64(&b.seek)
	read := atomic.LoadInt64(&b.read)
	decompress := atomic.LoadInt64(&b.decompress)
	diskRead := atomic.LoadInt64(&b.diskRead)
	numReads := atomic.LoadUint64(&b.numReads)
	sb := new(strings.Builder)
	sb.WriteString(fmt.Sprintf("Num Reads: %v\n", numReads))
	sb.WriteString(fmt.Sprintf("Disk Read: %v\n", time.Duration(diskRead)))
	sb.WriteString(fmt.Sprintf("Decompress: %v\n", time.Duration(decompress)))
	sb.WriteString(fmt.Sprintf("Seek Total: %v\n", time.Duration(seek)))
	sb.WriteString(fmt.Sprintf("Read Total: %v\n", time.Duration(read)))
	return sb.String()
}

func init() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP)
	go func() {
		for {
			<-sigs
			fmt.Println(bench)
		}
	}()

}
