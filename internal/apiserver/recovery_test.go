package apiserver

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryPersistsFailureOnlyForCurrentAllocation(t *testing.T) {
	ctx := context.Background()
	meta := store.NewMemoryMetadataStore()
	catalog := store.NewMemoryRuntimeCatalog()
	server := NewServer(ServerConfig{Meta: meta, Catalog: catalog})
	defer server.Close()
	sb := store.SandboxRecord{SandboxID: "box", ExecutionID: 1, CurrentNodeID: "node", CurrentSlotID: "slot", LifecycleID: "life-1", State: store.SandboxStateRunning, ActiveSnapshotID: "snapshot"}
	if err := meta.CreateSandbox(ctx, &sb); err != nil {
		t.Fatal(err)
	}
	old := sb
	route := protocol.RouteRecord{SandboxID: sb.SandboxID, ExecutionID: 1, NodeID: "node", SlotID: "slot", LifecycleID: sb.LifecycleID}
	report := store.ExecutionReport{State: store.OwnerFailed, Reason: "ArenaFS serving process exited"}
	if err := catalog.ReportExecution(ctx, route, report); err != nil {
		t.Fatal(err)
	}
	sb.ExecutionID++
	sb.LifecycleID = "life-2"
	if err := meta.SaveSandbox(ctx, &sb); err != nil {
		t.Fatal(err)
	}
	server.reconcileSandbox(ctx, &old)
	got, err := meta.GetSandbox(ctx, "box")
	if err != nil || got.State != store.SandboxStateRunning {
		t.Fatalf("stale observation changed execution: %v %v", got, err)
	}
	route.ExecutionID, route.LifecycleID = sb.ExecutionID, sb.LifecycleID
	if err := catalog.ReportExecution(ctx, route, report); err != nil {
		t.Fatal(err)
	}
	server.RunEvictionAndGCSweep(ctx)
	got, err = meta.GetSandbox(ctx, "box")
	if err != nil || got.State != store.SandboxStateFailed || got.CurrentSlotID != "slot" || got.ActiveSnapshotID != "snapshot" || got.FailureReason != report.Reason {
		t.Fatalf("lost failure evidence: %v %v", got, err)
	}
}

type checkpointReplayNode struct {
	protocol.NodeControlServer
	wantKind protocol.SnapshotKind
	result   string
	queries  atomic.Int32
	prepares atomic.Int32
}

func (n *checkpointReplayNode) GetLatestSnapshotResult(_ context.Context, req *protocol.GetLatestSnapshotResultRequest) (*protocol.GetLatestSnapshotResultResponse, error) {
	n.queries.Add(1)
	prep := req.Expected
	if req.SnapshotID != "pending" || prep == nil || prep.TargetSnapshotID != req.SnapshotID || prep.SandboxID != "box" || prep.ExecutionID != 2 || prep.LifecycleID != "life" || prep.SlotID != "slot" || prep.Kind != n.wantKind {
		return nil, status.Error(codes.InvalidArgument, "checkpoint probe has a different identity")
	}
	if n.result == "unavailable" {
		return nil, status.Error(codes.Unavailable, "node journal temporarily unavailable")
	}
	return &protocol.GetLatestSnapshotResultResponse{Found: n.result == "found", Manifest: protocol.SnapshotManifest{SnapshotID: req.SnapshotID}, ManifestURI: "file://pending"}, nil
}

func (n *checkpointReplayNode) PrepareCheckpoint(context.Context, *protocol.PrepareCheckpointRequest) (*protocol.PrepareCheckpointResponse, error) {
	n.prepares.Add(1)
	return nil, status.Error(codes.FailedPrecondition, "lost execution cannot capture again")
}

func TestLostOwnerRecoversPublishedCheckpointBeforeFailing(t *testing.T) {
	for _, tc := range []struct {
		name, result string
		kind         protocol.SnapshotKind
	}{
		{"published suspend", "found", protocol.SnapshotKindSessionSuspend},
		{"published fork", "found", protocol.SnapshotKindLiveFork},
		{"no capture", "missing", protocol.SnapshotKindSessionSuspend},
		{"worker temporarily unavailable", "unavailable", protocol.SnapshotKindSessionSuspend},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			listen, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			node := &checkpointReplayNode{wantKind: tc.kind, result: tc.result}
			worker := grpc.NewServer()
			defer worker.Stop()
			protocol.RegisterNodeControlServer(worker, node)
			go func() { _ = worker.Serve(listen) }()
			meta := store.NewMemoryMetadataStore()
			catalog := store.NewMemoryRuntimeCatalog()
			server := NewServer(ServerConfig{Meta: meta, Catalog: catalog})
			defer server.Close()
			box := &store.SandboxRecord{SandboxID: "box", ExecutionID: 2, LifecycleID: "life", CurrentSlotID: "slot", CurrentNodeID: "node", PodIP: "10.0.0.1", State: store.SandboxStateSuspending, PendingSnapshotID: "pending"}
			if tc.kind == protocol.SnapshotKindLiveFork {
				box.State, box.PendingSnapshotID = store.SandboxStateRunning, ""
				box.PendingFork = &store.PendingFork{SnapshotID: "pending"}
			}
			if err := meta.CreateSandbox(ctx, box); err != nil {
				t.Fatal(err)
			}
			if err := catalog.UpsertNodeHeartbeat(ctx, protocol.NodeHeartbeat{NodeID: "node", GRPCAddr: listen.Addr().String()}, time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := catalog.ReportExecution(ctx, protocol.RouteRecord{SandboxID: "box", ExecutionID: 2, LifecycleID: "life", SlotID: "slot", NodeID: "node"}, store.ExecutionReport{State: store.OwnerFailed, Reason: "restore view lost"}); err != nil {
				t.Fatal(err)
			}
			if stopped := server.reconcileSandbox(ctx, box); stopped != (tc.result != "found") {
				t.Fatalf("unexpected last capture decision: stopped=%t", stopped)
			}
			if node.queries.Load() != 1 || node.prepares.Load() != 0 {
				t.Fatalf("reconciliation did not use only the stored result: queries=%d prepares=%d", node.queries.Load(), node.prepares.Load())
			}
			state := box.State
			pending := "pending"
			if tc.result == "found" {
				pending = ""
				snapshot, err := meta.GetSnapshot(ctx, "pending")
				if err != nil || snapshot.Kind != tc.kind || snapshot.ManifestURI != "file://pending" {
					t.Fatalf("did not publish replayed snapshot: got=%+v err=%v", snapshot, err)
				}
			} else if tc.result == "missing" {
				state = store.SandboxStateFailed
			}
			got, err := meta.GetSandbox(ctx, "box")
			if err != nil || got.State != state {
				t.Fatalf("unexpected current state: got=%+v err=%v", got, err)
			}
			if got, err := meta.UnpublishedSnapshot(ctx); err != nil || got != pending {
				t.Fatalf("unexpected collection barrier: pending=%q err=%v", got, err)
			}
		})
	}
}

func TestRouteRepairCannotWakeSuspendedSandbox(t *testing.T) {
	ctx := context.Background()
	meta := store.NewMemoryMetadataStore()
	catalog := store.NewMemoryRuntimeCatalog()
	server := NewServer(ServerConfig{Meta: meta, Catalog: catalog})
	defer server.Close()
	observed := store.SandboxRecord{SandboxID: "box", State: store.SandboxStateRunning, ExecutionID: 1, LifecycleID: "life", CurrentNodeID: "node", CurrentSlotID: "slot", ActiveSnapshotID: "checkpoint"}
	suspended := observed
	suspended.State = store.SandboxStateSuspended
	suspended.CurrentSlotID = ""
	if err := meta.CreateSandbox(ctx, &suspended); err != nil {
		t.Fatal(err)
	}
	route := protocol.RouteRecord{SandboxID: observed.SandboxID, ExecutionID: observed.ExecutionID, LifecycleID: observed.LifecycleID, NodeID: observed.CurrentNodeID, SlotID: observed.CurrentSlotID}
	if err := catalog.ReportExecution(ctx, route, store.ExecutionReport{State: store.OwnerReady}); err != nil {
		t.Fatal(err)
	}
	server.reconcileSandbox(ctx, &observed)
	got, err := meta.GetSandbox(ctx, observed.SandboxID)
	if err != nil || got.State != store.SandboxStateSuspended || got.ExecutionID != observed.ExecutionID || got.LifecycleID != observed.LifecycleID {
		t.Fatalf("stale route repair advanced a paused execution: got=%+v err=%v", got, err)
	}
}

func TestRouteRepairRepublishesOnlyTheVerifiedRunningExecution(t *testing.T) {
	ctx := context.Background()
	meta := store.NewMemoryMetadataStore()
	catalog := store.NewMemoryRuntimeCatalog()
	server := NewServer(ServerConfig{Meta: meta, Catalog: catalog})
	defer server.Close()
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(protocol.HealthResponse{SandboxID: "box", ExecutionID: 1, LifecycleID: "life", Healthy: true})
	}))
	defer guest.Close()
	box := store.SandboxRecord{SandboxID: "box", State: store.SandboxStateRunning, ExecutionID: 1, LifecycleID: "life", CurrentNodeID: "node", CurrentSlotID: "slot", PodIP: strings.TrimPrefix(guest.URL, "http://"), AccessToken: "box-token"}
	if err := meta.CreateSandbox(ctx, &box); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertNodeHeartbeat(ctx, protocol.NodeHeartbeat{NodeID: "node", GRPCAddr: "node:8081"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	route := protocol.RouteRecord{SandboxID: box.SandboxID, ExecutionID: box.ExecutionID, LifecycleID: box.LifecycleID, NodeID: box.CurrentNodeID, SlotID: box.CurrentSlotID}
	if err := catalog.ReportExecution(ctx, route, store.ExecutionReport{State: store.OwnerReady}); err != nil {
		t.Fatal(err)
	}
	server.reconcileSandbox(ctx, &box)
	got, err := catalog.GetRoute(ctx, box.SandboxID)
	if err != nil || got.ExecutionID != box.ExecutionID || got.AccessDigest != protocol.AccessDigest(box.AccessToken) || got.OwnerKey != store.OwnerKey(route) {
		t.Fatalf("verified route was not repaired: got=%+v err=%v", got, err)
	}
}
