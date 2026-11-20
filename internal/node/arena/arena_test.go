package arena_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/internal/node/p2p"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"google.golang.org/grpc"
)

func TestThreeTierArenaP2PLazyPageFetchAndRustHelper(t *testing.T) {
	ctx := context.Background()
	sharedCAS, err := storage.NewLocalFSBackend(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFSBackend: %v", err)
	}

	// Start Node A (OriginNode) with its own Arena + P2P server!
	p2pLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	rustHelper := filepath.Join("..", "..", "..", "bin", "kontinue-arena-helper")
	if _, statErr := os.Stat(rustHelper); statErr != nil {
		rustHelper = ""
	}

	nodeA, err := arena.NewManager(arena.ManagerConfig{
		NodeID:          "node-a",
		P2PAddr:         p2pLis.Addr().String(),
		L1TmpfsDir:      t.TempDir(),
		L2CASDir:        t.TempDir(),
		ViewsDir:        t.TempDir(),
		RustArenaHelper: rustHelper,
		RemoteStore:     sharedCAS,
	})
	if err != nil {
		t.Fatalf("NewManager nodeA: %v", err)
	}
	defer nodeA.Close()

	p2pSrv := grpc.NewServer()
	protocol.RegisterNodeArenaP2PServer(p2pSrv, p2p.NewServer(nodeA))
	go func() { _ = p2pSrv.Serve(p2pLis) }()
	defer p2pSrv.Stop()

	// Create a 6 MiB raw gVisor checkpoint on Node A (Chunk 0 = 'X', Chunk 1 = Zero, Chunk 2 = 'Y').
	rawDir := t.TempDir()
	pages := make([]byte, 3*protocol.DefaultChunkSize)
	for i := 0; i < protocol.DefaultChunkSize; i++ {
		pages[i] = 'X'
		pages[2*protocol.DefaultChunkSize+i] = 'Y'
	}
	_ = os.WriteFile(filepath.Join(rawDir, protocol.GVisorCheckpointImg), []byte("sentry-kernel"), 0o600)
	_ = os.WriteFile(filepath.Join(rawDir, protocol.GVisorPagesMetaImg), []byte("pages-meta"), 0o600)
	_ = os.WriteFile(filepath.Join(rawDir, protocol.GVisorPagesImg), pages, 0o600)

	tarOut := filepath.Join(t.TempDir(), "kubelet-ckpt.tar")
	mf, _, err := nodeA.IngestRawCheckpoint(ctx, arena.IngestOptions{
		SnapshotID:       "snap-cross-node-1",
		Kind:             protocol.SnapshotKindSessionSuspend,
		BaseImage:        "alpine:3.20",
		RawCheckpointDir: rawDir,
		KubeletTarPath:   tarOut,
		TraceHotPages:    true,
	})
	if err != nil {
		t.Fatalf("IngestRawCheckpoint: %v", err)
	}
	if mf.ZeroChunkCount != 1 || mf.DirtyChunkCount != 2 {
		t.Fatalf("unexpected chunk stats: %+v", mf)
	}
	if _, err := os.Stat(tarOut); err != nil {
		t.Fatalf("expected Kubelet tar archive written: %v", err)
	}

	// Start Node B (empty local cache!) and exercise LazyPageReader fetching chunks from Node A via P2P!
	nodeBCAS := t.TempDir()
	lazyReader, err := arena.NewLazyPageReader(arena.LazyReaderOptions{
		Manifest:       mf,
		LocalCASDir:    nodeBCAS,
		BackingFile:    filepath.Join(t.TempDir(), "lazy.pages"),
		RemoteStore:    sharedCAS,
		OriginPeerAddr: p2pLis.Addr().String(),
		TraceAccess:    true,
	})
	if err != nil {
		t.Fatalf("NewLazyPageReader: %v", err)
	}
	defer lazyReader.Close()

	// Fault into the middle of Chunk 2 ('Y') before Chunk 0 is loaded!
	probe := make([]byte, 64)
	off := int64(2*protocol.DefaultChunkSize + 128)
	n, err := lazyReader.ReadAt(ctx, probe, off)
	if err != nil || n != 64 || !bytes.Equal(probe, bytes.Repeat([]byte{'Y'}, 64)) {
		t.Fatalf("lazy fault read mismatch: n=%d err=%v", n, err)
	}
	hotRecorded := lazyReader.RecordedHotPages()
	if len(hotRecorded) != 1 || hotRecorded[0] != 2 {
		t.Fatalf("expected first faulted chunk index [2], got %v", hotRecorded)
	}
}
