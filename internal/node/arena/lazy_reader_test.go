package arena

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
)

type gatedBlob struct {
	storage.Backend
	entered chan struct{}
	release chan struct{}
}

func (b gatedBlob) GetBlob(ctx context.Context, key string) ([]byte, error) {
	b.entered <- struct{}{}
	select {
	case <-b.release:
		return []byte("same"), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestConcurrentFaultsWithRepeatedChunkAtDifferentOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote := gatedBlob{entered: make(chan struct{}, 2), release: make(chan struct{})}
	mf := &protocol.SnapshotManifest{PagesImgTotalSize: 8, ChunkSize: 4, Chunks: []protocol.ChunkRef{
		{Offset: 0, Length: 4, Blake3Hash: "repeated"}, {Offset: 4, Length: 4, Blake3Hash: "repeated"},
	}}
	r, err := NewLazyPageReader(LazyReaderOptions{Manifest: mf, LocalCASDir: t.TempDir(), BackingFile: filepath.Join(t.TempDir(), "pages"), RemoteStore: remote})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	results := make(chan error, 2)
	for _, off := range []int64{0, 4} {
		go func() {
			buf := make([]byte, 4)
			n, err := r.ReadAt(ctx, buf, off)
			if err == nil && (n != 4 || string(buf) != "same") {
				err = fmt.Errorf("offset %d: read %d bytes %q", off, n, buf)
			}
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-remote.entered:
		case <-ctx.Done():
			t.Fatal("two distinct offsets did not enter materialization")
		}
	}
	close(remote.release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if r.copiedBytes.Load() != 8 {
		t.Fatalf("copied %d bytes", r.copiedBytes.Load())
	}
}
