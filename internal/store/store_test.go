package store_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func TestExecutionIDFencingAndSnapshotGC(t *testing.T) {
	ctx := context.Background()
	cat := store.NewMemoryRuntimeCatalog()
	meta := store.NewMemoryMetadataStore()

	// 1. Register & atomically claim Carrier Slot
	if err := cat.RegisterIdleSlot(ctx, protocol.CarrierSlot{
		SlotID: "slot-1",
		NodeID: "node-1",
		Tier:   protocol.DefaultCarrierTier,
		PodIP:  "10.244.1.10",
	}); err != nil {
		t.Fatalf("RegisterIdleSlot: %v", err)
	}
	slot, err := cat.ClaimIdleSlot(ctx, "node-1", protocol.DefaultCarrierTier)
	if err != nil || slot.SlotID != "slot-1" {
		t.Fatalf("ClaimIdleSlot got (%+v, %v)", slot, err)
	}
	if _, err := cat.ClaimIdleSlot(ctx, "node-1", protocol.DefaultCarrierTier); !errors.Is(err, store.ErrNoIdleCarrierSlot) {
		t.Fatalf("expected ErrNoIdleCarrierSlot on empty pool, got %v", err)
	}

	// 2. ExecutionID Fencing: execution=2 publishes route; stale execution=1 cannot overwrite or delete it!
	if err := cat.PublishRoute(ctx, protocol.RouteRecord{SandboxID: "sbx-1", ExecutionID: 2, PodIP: "10.244.1.11"}); err != nil {
		t.Fatalf("PublishRoute v2: %v", err)
	}
	if err := cat.PublishRoute(ctx, protocol.RouteRecord{SandboxID: "sbx-1", ExecutionID: 1, PodIP: "10.244.1.9"}); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatalf("expected stale PublishRoute v1 to fail with ErrStaleExecutionFence, got %v", err)
	}
	if err := cat.DeleteRouteFenced(ctx, "sbx-1", 1, ""); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatalf("expected stale DeleteRouteFenced v1 to fail, got %v", err)
	}
	r, err := cat.GetRoute(ctx, "sbx-1")
	if err != nil || r.PodIP != "10.244.1.11" {
		t.Fatalf("expected v2 route intact, got (%+v, %v)", r, err)
	}

	// 3. Reference-Counted Snapshot GC: superseded snapshot referenced by child diff is kept; unreferenced is collected!
	_ = meta.CreateSnapshot(ctx, &store.SnapshotRecord{SnapshotID: "snap-v1", Superseded: true, CreatedAt: time.Now()})
	_ = meta.CreateSnapshot(ctx, &store.SnapshotRecord{SnapshotID: "snap-v2", ParentSnapshotID: "snap-v1", Superseded: false, CreatedAt: time.Now()})
	_ = meta.CreateSnapshot(ctx, &store.SnapshotRecord{SnapshotID: "snap-orphan", Superseded: true, CreatedAt: time.Now()})

	gcList, err := meta.ListGCandidateSnapshots(ctx)
	if err != nil || len(gcList) != 1 || gcList[0].SnapshotID != "snap-orphan" {
		t.Fatalf("expected only snap-orphan eligible for GC, got %+v (err=%v)", gcList, err)
	}
}

func TestLeaderLockAndInflight(t *testing.T) {
	ctx := context.Background()
	meta := store.NewMemoryMetadataStore()
	rel, ok, err := meta.TryLeader(ctx, protocol.LeaderEvictor)
	if err != nil || !ok {
		t.Fatalf("first leader: ok=%v err=%v", ok, err)
	}
	if _, ok2, err2 := meta.TryLeader(ctx, protocol.LeaderEvictor); err2 != nil || ok2 {
		t.Fatalf("second leader should lose: ok=%v err=%v", ok2, err2)
	}
	rel()
	if _, ok3, _ := meta.TryLeader(ctx, protocol.LeaderEvictor); !ok3 {
		t.Fatal("leader should be free after release")
	}

	cat := store.NewMemoryRuntimeCatalog()
	n, err := cat.AdjustInflight(ctx, "sbx", 1)
	if err != nil || n != 1 {
		t.Fatalf("inflight +1: %d %v", n, err)
	}
	got, _ := cat.Inflight(ctx, "sbx")
	if got != 1 {
		t.Fatalf("inflight read %d", got)
	}
	if n, _ = cat.AdjustInflight(ctx, "sbx", -1); n != 0 {
		t.Fatalf("inflight back to 0, got %d", n)
	}
}

func TestPostgresPublicationBarrierAcrossReplicas(t *testing.T) {
	dsn := os.Getenv("KONTINUE_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set KONTINUE_TEST_POSTGRES to a disposable PostgreSQL database")
	}
	ctx := context.Background()
	connect := func() *store.PostgresMetadataStore {
		s, err := store.NewPostgresMetadataStore(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	publisher, collector := connect(), connect()
	done, err := publisher.LockSnapshotSet(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	release, err := collector.LockSnapshotSet(bounded, true)
	cancel()
	done()
	if err == nil {
		release()
		t.Fatal("exclusive collector overlapped active publisher")
	}
	release, err = collector.LockSnapshotSet(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	release()
}
