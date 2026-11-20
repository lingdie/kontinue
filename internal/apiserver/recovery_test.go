package apiserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
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
