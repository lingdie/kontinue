package apiserver

import (
	"context"
	"encoding/json"
	"errors"
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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

type releaseReplayNode struct {
	protocol.NodeControlServer
	calls atomic.Int32
}

func (n *releaseReplayNode) ReleaseSlot(_ context.Context, req *protocol.ReleaseSlotRequest) (*protocol.ReleaseSlotResponse, error) {
	if req.SandboxID != "box" || req.ExecutionID != 2 || req.LifecycleID != "life" || req.SlotID != "slot" {
		return nil, errors.New("wrong release identity")
	}
	n.calls.Add(1)
	return &protocol.ReleaseSlotResponse{Released: true, PodUID: "original-uid"}, nil
}

func TestSuspendReleaseRetainsAssignmentUntilPodDeletionIsConfirmed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var deletes atomic.Int32
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v1/namespaces/carriers/pods/pod" {
			t.Errorf("unexpected pod path %s", r.URL.Path)
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{UID: "replacement-uid"}})
			return
		}
		var opts metav1.DeleteOptions
		if err := json.NewDecoder(r.Body).Decode(&opts); r.Method != http.MethodDelete || err != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "original-uid" {
			t.Errorf("pod deletion lost its original UID: method=%s opts=%+v err=%v", r.Method, opts, err)
		}
		code, reason := http.StatusConflict, metav1.StatusReasonConflict
		if deletes.Add(1) == 1 {
			code, reason = http.StatusInternalServerError, metav1.StatusReasonInternalError
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Code: int32(code), Reason: reason})
	}))
	defer kube.Close()
	kubeClient, err := kubernetes.NewForConfig(&rest.Config{Host: kube.URL})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &releaseReplayNode{}
	worker := grpc.NewServer()
	defer worker.Stop()
	protocol.RegisterNodeControlServer(worker, node)
	go func() { _ = worker.Serve(lis) }()
	meta := store.NewMemoryMetadataStore()
	catalog := store.NewMemoryRuntimeCatalog()
	box := &store.SandboxRecord{SandboxID: "box", State: store.SandboxStateSuspended, ExecutionID: 2, LifecycleID: "life", CurrentNodeID: "node", CurrentSlotID: "slot", CarrierPodNamespace: "carriers", CarrierPodName: "pod", PodIP: "10.0.0.1"}
	if err := meta.CreateSandbox(ctx, box); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertNodeHeartbeat(ctx, protocol.NodeHeartbeat{NodeID: "node", GRPCAddr: lis.Addr().String()}, time.Minute); err != nil {
		t.Fatal(err)
	}
	cfg := ServerConfig{Meta: meta, Catalog: catalog, Replenisher: NewCarrierPoolReplenisher(kubeClient, catalog, ReplenisherConfig{})}
	first := NewServer(cfg)
	err = first.finishSuspendRelease(ctx, box)
	first.Close()
	if err == nil {
		t.Fatal("forgot a transient pod deletion failure")
	}
	recovered := NewServer(cfg)
	defer recovered.Close()
	box, err = meta.GetSandbox(ctx, "box")
	if err != nil || box.CurrentSlotID != "slot" {
		t.Fatalf("old slot unrecorded after API replacement: got=%+v err=%v", box, err)
	}
	if err := recovered.finishSuspendRelease(ctx, box); err != nil {
		t.Fatalf("retry against a replacement Pod did not finish: %v", err)
	}
	box, err = meta.GetSandbox(ctx, "box")
	if err != nil || box.CurrentSlotID != "" || node.calls.Load() != 2 || deletes.Load() != 2 {
		t.Fatalf("unexpected release replay: got=%+v err=%v releases=%d deletes=%d", box, err, node.calls.Load(), deletes.Load())
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

func (n *checkpointReplayNode) ReleaseSlot(context.Context, *protocol.ReleaseSlotRequest) (*protocol.ReleaseSlotResponse, error) {
	return &protocol.ReleaseSlotResponse{Released: true, PodUID: "pod"}, nil
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
				if tc.kind == protocol.SnapshotKindSessionSuspend {
					state = store.SandboxStateSuspended
				}
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
			if tc.result == "found" && tc.kind == protocol.SnapshotKindSessionSuspend && (got.PendingSnapshotID != "" || got.CurrentSlotID != "") {
				t.Fatalf("recovered suspend did not release the old assignment: %+v", got)
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

func TestDeleteCancelsPendingPlacementWithoutReleaseRPC(t *testing.T) {
	ctx := context.Background()
	meta := store.NewMemoryMetadataStore()
	catalog := store.NewMemoryRuntimeCatalog()
	server := NewServer(ServerConfig{Meta: meta, Catalog: catalog})
	defer server.Close()
	slot := protocol.CarrierSlot{SlotID: "slot", NodeID: "node", Tier: protocol.DefaultCarrierTier}
	if err := catalog.RegisterIdleSlot(ctx, slot); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.ReserveIdleSlot(ctx, slot.NodeID, slot.Tier, "life", false); err != nil {
		t.Fatal(err)
	}
	box := &store.SandboxRecord{
		SandboxID: "pending", State: store.SandboxStateCreating, ExecutionID: 1, LifecycleID: "life",
		CurrentNodeID: slot.NodeID, CurrentSlotID: slot.SlotID,
		PendingPlacement: &store.ActivationPlacement{Slot: slot, RestoreMode: protocol.RestoreModeCold},
	}
	if err := meta.CreateSandbox(ctx, box); err != nil {
		t.Fatal(err)
	}
	if err := server.DeleteSandbox(ctx, box.SandboxID); err != nil {
		t.Fatal(err)
	}
	if _, err := meta.GetSandbox(ctx, box.SandboxID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("pending sandbox survived cancellation: %v", err)
	}
	reclaimed, err := catalog.ReserveIdleSlot(ctx, slot.NodeID, slot.Tier, "retry", false)
	if err != nil || reclaimed.Slot.SlotID != slot.SlotID {
		t.Fatalf("pending reservation was not returned to the pool: slot=%+v err=%v", reclaimed, err)
	}
}
