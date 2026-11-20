package apiserver

import (
	"context"
	"testing"

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
