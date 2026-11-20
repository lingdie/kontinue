package hydrator

import (
	"archive/tar"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func TestCompletedCheckpointReplaysAfterDaemonReplacement(t *testing.T) {
	ctx := context.Background()
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.PathInternalHealth {
			t.Errorf("unexpected guest request %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"healthy":true,"initialized":true,"sandboxId":"box","executionId":2}`))
	}))
	defer guest.Close()
	manager, err := arena.NewManager(arena.ManagerConfig{L2CASDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	cfg := ServiceConfig{ScratchDir: t.TempDir(), NodeID: "worker", Driver: &inspectingDriver{}, Arena: manager}
	prep := protocol.PrepareCheckpointRequest{TargetSnapshotID: "snap", SandboxID: "box", SlotID: "slot", ExecutionID: 2, LifecycleID: "life", Kind: protocol.SnapshotKindLiveFork}
	saved := checkpointReceipt{RequestHash: checkpointRequestHash(&prep), Result: protocol.ExecuteCheckpointResponse{Manifest: protocol.SnapshotManifest{SnapshotID: "snap"}, ManifestURI: "local://manifest"}}
	claim := ownership{Version: 1, Phase: activeExecution, Slot: protocol.CarrierSlot{NodeID: "worker", SlotID: "slot"}, ContainerID: "kt-original", SandboxID: "box", ExecutionID: 2, LifecycleID: "life", KtdEndpoint: strings.TrimPrefix(guest.URL, "http://"), LastCheckpoint: &saved}
	if err := NewService(cfg).journal.write(claim, true); err != nil {
		t.Fatal(err)
	}
	s := NewService(cfg)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	query := protocol.GetLatestSnapshotResultRequest{SnapshotID: "snap", Expected: &prep}
	got, err := s.GetLatestSnapshotResult(ctx, &query)
	if err != nil || !got.Found || got.ManifestURI != saved.Result.ManifestURI {
		t.Fatalf("receipt missing: %v", err)
	}
	if _, err := s.PrepareCheckpoint(ctx, &prep); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "replayed.tar")
	if _, err := s.ExecuteCheckpoint(ctx, &protocol.ExecuteCheckpointRequest{ContainerID: claim.ContainerID, KubeletLocation: path}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := tar.NewReader(f).Next()
	if err != nil || h.Name != "kontinue-descriptor.json" {
		t.Fatalf("missing descriptor: %v", err)
	}
	prep.ExecutionID++
	if _, err := s.GetLatestSnapshotResult(ctx, &query); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatalf("receipt accepted a different generation: %v", err)
	}
}
