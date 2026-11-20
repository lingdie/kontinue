package arena

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LazyPageReader backs L3 ArenaFS virtual pages.img files:
// - Serves zero-page holes at 0 I/O cost;
// - Deduplicates concurrent page faults on the same 2MiB chunk via singleflight;
// - Fetches missing chunks in priority order: Local L2 CAS -> OriginNode P2P -> S3/CAS Backend;
// - Optionally records first-touch chunk indices to generate hot-pages.idx during template build;
// - Runs a bounded background Hot-Page Prefetcher.
type LazyPageReader struct {
	manifest    *protocol.SnapshotManifest
	localCASDir string
	remoteStore storage.Backend
	sparseFile  *os.File
	ready       []atomic.Bool
	sf          singleflight.Group

	peerConn   *grpc.ClientConn
	peerClient *protocol.NodeArenaP2PClient

	traceAccess   bool
	traceMu       sync.Mutex
	seenFault     map[uint32]bool
	faultSequence []uint32

	cancelPrefetch context.CancelFunc
	prefetchWG     sync.WaitGroup
	closeOnce      sync.Once
}

type LazyReaderOptions struct {
	Manifest       *protocol.SnapshotManifest
	LocalCASDir    string
	BackingFile    string
	RemoteStore    storage.Backend
	OriginPeerAddr string
	TraceAccess    bool
}

func NewLazyPageReader(opts LazyReaderOptions) (*LazyPageReader, error) {
	if opts.Manifest == nil {
		return nil, fmt.Errorf("manifest is required")
	}
	if err := os.MkdirAll(filepath.Dir(opts.BackingFile), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(opts.BackingFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(opts.Manifest.PagesImgTotalSize); err != nil {
		_ = f.Close()
		return nil, err
	}

	r := &LazyPageReader{
		manifest:    opts.Manifest,
		localCASDir: opts.LocalCASDir,
		remoteStore: opts.RemoteStore,
		sparseFile:  f,
		ready:       make([]atomic.Bool, len(opts.Manifest.Chunks)),
		traceAccess: opts.TraceAccess,
		seenFault:   make(map[uint32]bool),
	}
	for i, ch := range opts.Manifest.Chunks {
		if ch.ZeroChunk {
			r.ready[i].Store(true)
		}
	}

	if opts.OriginPeerAddr != "" {
		if cc, dialErr := grpc.NewClient(opts.OriginPeerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(telemetry.UnaryClientInterceptor("arena-lazy-p2p")),
			grpc.WithStreamInterceptor(telemetry.StreamClientInterceptor("arena-lazy-p2p")),
		); dialErr == nil {
			r.peerConn = cc
			r.peerClient = protocol.NewNodeArenaP2PClient(cc)
		}
	}
	return r, nil
}

func (r *LazyPageReader) Size() int64 {
	return r.manifest.PagesImgTotalSize
}

func (r *LazyPageReader) SnapshotID() string {
	if r.manifest == nil {
		return ""
	}
	return r.manifest.SnapshotID
}

func (r *LazyPageReader) RecordedHotPages() []uint32 {
	r.traceMu.Lock()
	defer r.traceMu.Unlock()
	return append([]uint32(nil), r.faultSequence...)
}

func (r *LazyPageReader) recordTouch(chunkIdx uint32) {
	if !r.traceAccess {
		return
	}
	r.traceMu.Lock()
	if !r.seenFault[chunkIdx] {
		r.seenFault[chunkIdx] = true
		r.faultSequence = append(r.faultSequence, chunkIdx)
	}
	r.traceMu.Unlock()
}

// ReadAt satisfies random pread() calls from gVisor's AsyncMFLoader via ArenaFS FUSE.
func (r *LazyPageReader) ReadAt(ctx context.Context, dest []byte, off int64) (int, error) {
	totalSize := r.manifest.PagesImgTotalSize
	if off >= totalSize {
		return 0, io.EOF
	}
	maxRead := int64(len(dest))
	if off+maxRead > totalSize {
		maxRead = totalSize - off
	}
	if maxRead <= 0 {
		return 0, nil
	}

	chunkSize := int64(r.manifest.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = protocol.DefaultChunkSize
	}
	firstIdx := int(off / chunkSize)
	lastIdx := int((off + maxRead - 1) / chunkSize)
	if lastIdx >= len(r.manifest.Chunks) {
		lastIdx = len(r.manifest.Chunks) - 1
	}

	for idx := firstIdx; idx <= lastIdx; idx++ {
		r.recordTouch(uint32(idx))
		if err := r.ensureChunk(ctx, idx); err != nil {
			return 0, err
		}
	}

	n, err := r.sparseFile.ReadAt(dest[:maxRead], off)
	if err != nil && err != io.EOF {
		return n, err
	}
	if off+int64(n) >= totalSize && n < len(dest) {
		return n, io.EOF
	}
	return n, nil
}

func (r *LazyPageReader) ensureChunk(ctx context.Context, idx int) error {
	if idx < 0 || idx >= len(r.manifest.Chunks) {
		return nil
	}
	if r.ready[idx].Load() {
		return nil
	}
	ch := r.manifest.Chunks[idx]
	if ch.ZeroChunk || ch.Blake3Hash == "" {
		r.ready[idx].Store(true)
		return nil
	}

	ctx, span := telemetry.StartSpan(ctx, "kontinue-arena", "arena.page_fault",
		attribute.String(telemetry.AttrSnapshotID, r.manifest.SnapshotID),
		attribute.Int(telemetry.AttrChunkIndex, idx),
		attribute.String(telemetry.AttrChunkHash, ch.Blake3Hash),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	val, err, _ := r.sf.Do(ch.Blake3Hash, func() (any, error) {
		if r.ready[idx].Load() {
			return "already_ready", nil
		}
		data, source, fetchErr := r.fetchChunkBytes(ctx, ch.Blake3Hash)
		if fetchErr != nil {
			return "", fetchErr
		}
		if _, wErr := r.sparseFile.WriteAt(data, ch.Offset); wErr != nil {
			return "", wErr
		}
		r.ready[idx].Store(true)
		return source, nil
	})
	if err != nil {
		retErr = err
		return err
	}
	if src, ok := val.(string); ok {
		span.SetAttributes(attribute.String(telemetry.AttrChunkSource, src))
	}
	return nil
}

func (r *LazyPageReader) fetchChunkBytes(ctx context.Context, hash string) ([]byte, string, error) {
	localPath := filepath.Join(r.localCASDir, storage.ChunkKey(hash))
	if data, err := os.ReadFile(localPath); err == nil {
		return data, "local_cas", nil
	}

	// Priority 1: Stream directly from OriginNode's L1/L2 Arena over gRPC P2P.
	if r.peerClient != nil {
		if resp, err := r.peerClient.GetChunk(ctx, &protocol.GetChunkRequest{Blake3Hash: hash}); err == nil && len(resp.Data) > 0 {
			_ = writeAtomicFile(localPath, resp.Data)
			return resp.Data, "p2p_peer", nil
		}
	}

	// Priority 2: Fetch from durable Object Storage CAS (S3/MinIO).
	if r.remoteStore != nil {
		data, err := r.remoteStore.GetBlob(ctx, storage.ChunkKey(hash))
		if err == nil {
			_ = writeAtomicFile(localPath, data)
			return data, "s3_cas", nil
		}
		return nil, "", fmt.Errorf("fetch chunk %s from remote CAS: %w", hash, err)
	}

	return nil, "", fmt.Errorf("chunk %s unavailable in local CAS, P2P, or remote store", hash)
}

// StartHotPagePrefetcher proactively warms known hot pages recorded in manifest.HotPageIndices
// followed by remaining cold pages in the background without blocking foreground faults.
func (r *LazyPageReader) StartHotPagePrefetcher(parentCtx context.Context, workers int) {
	if workers <= 0 {
		workers = 4
	}
	ctx, cancel := context.WithCancel(parentCtx)
	r.cancelPrefetch = cancel

	order := make([]int, 0, len(r.manifest.Chunks))
	queued := make(map[int]bool, len(r.manifest.Chunks))
	for _, hotIdx := range r.manifest.HotPageIndices {
		i := int(hotIdx)
		if i >= 0 && i < len(r.manifest.Chunks) && !queued[i] {
			queued[i] = true
			order = append(order, i)
		}
	}
	for i := range r.manifest.Chunks {
		if !queued[i] {
			order = append(order, i)
		}
	}

	ch := make(chan int, len(order))
	for _, idx := range order {
		if !r.ready[idx].Load() {
			ch <- idx
		}
	}
	close(ch)

	for w := 0; w < workers; w++ {
		r.prefetchWG.Add(1)
		go func() {
			defer r.prefetchWG.Done()
			for idx := range ch {
				if ctx.Err() != nil {
					return
				}
				_ = r.ensureChunk(ctx, idx)
			}
		}()
	}
}

// WarmAll eagerly populates all non-zero chunks into the backing sparse file.
func (r *LazyPageReader) WarmAll(ctx context.Context) error {
	for i := range r.manifest.Chunks {
		if err := r.ensureChunk(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (r *LazyPageReader) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		if r.cancelPrefetch != nil {
			r.cancelPrefetch()
		}
		r.prefetchWG.Wait()
		if r.peerConn != nil {
			_ = r.peerConn.Close()
		}
		if r.sparseFile != nil {
			name := r.sparseFile.Name()
			dir := filepath.Dir(name)
			closeErr = r.sparseFile.Close()
			_ = os.Remove(name)
			// backing.pages lives in staging/<viewID>/. The staging root itself stays.
			if filepath.Base(filepath.Dir(dir)) == "staging" {
				_ = os.RemoveAll(dir)
			}
		}
	})
	return closeErr
}

func writeAtomicFile(dst string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", dst, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
