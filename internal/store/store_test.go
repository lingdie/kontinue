package store_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
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

func TestFailedOwnershipCannotForwardOrResurrect(t *testing.T) {
	t.Run("memory", func(t *testing.T) { checkFailedOwnership(t, store.NewMemoryRuntimeCatalog()) })
	t.Run("redis", func(t *testing.T) {
		addr := os.Getenv("KONTINUE_TEST_REDIS")
		if addr == "" {
			t.Skip("KONTINUE_TEST_REDIS is unset")
		}
		cat := store.NewRedisRuntimeCatalog(addr, "", 0)
		defer cat.Close()
		checkFailedOwnership(t, cat)
	})
}

func checkFailedOwnership(t *testing.T, cat store.RuntimeCatalog) {
	ctx := context.Background()
	id := "recovery-" + rand.Text()
	old := protocol.RouteRecord{SandboxID: id, ExecutionID: 1, LifecycleID: "old", SlotID: "a", NodeID: "worker"}
	old.OwnerKey = store.OwnerKey(old)
	if err := cat.PublishRoute(ctx, old); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatal(err)
	}
	if err := cat.ReportExecution(ctx, old, store.ExecutionReport{State: store.OwnerReady}); err != nil {
		t.Fatal(err)
	}
	if err := cat.PublishRoute(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.AdmitRoute(ctx, id, "in-flight"); err != nil {
		t.Fatal(err)
	}
	fail := store.ExecutionReport{State: store.OwnerFailed, Reason: "ArenaFS serving process exited"}
	if err := cat.ReportExecution(ctx, old, fail); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.AdmitRoute(ctx, id, "late"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if err := cat.RenewAdmission(ctx, id, "in-flight"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if err := cat.PublishRoute(ctx, old); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatal(err)
	}
	next := old
	next.ExecutionID++
	next.LifecycleID = "new"
	next.OwnerKey = store.OwnerKey(next)
	if err := cat.ReportExecution(ctx, next, store.ExecutionReport{State: store.OwnerReady}); err != nil {
		t.Fatal(err)
	}
	if err := cat.PublishRoute(ctx, next); err != nil {
		t.Fatal(err)
	}
	if err := cat.ReportExecution(ctx, old, fail); err != nil {
		t.Fatal(err)
	}
	if r, err := cat.AdmitRoute(ctx, id, "current"); err != nil || r.ExecutionID != 2 {
		t.Fatalf("new route=%v err=%v", r, err)
	}
}

func TestPlacementClaimReplaysAcrossSchedulingDecisions(t *testing.T) {
	check := func(t *testing.T, cat store.RuntimeCatalog) {
		ctx := context.Background()
		node := "worker-" + rand.Text()
		life := "life-" + rand.Text()
		slot := protocol.CarrierSlot{SlotID: "slot-" + rand.Text(), NodeID: node, Tier: "default", PodUID: "original"}
		if err := cat.RegisterIdleSlot(ctx, slot); err != nil {
			t.Fatal(err)
		}
		first, err := cat.ReserveIdleSlot(ctx, node, slot.Tier, life, true)
		if err != nil || !first.Cold {
			t.Fatal(first, err)
		}
		if err := cat.RegisterIdleSlot(ctx, slot); err != nil {
			t.Fatal(err)
		}
		if count, err := cat.CountIdleSlots(ctx, node, slot.Tier); err != nil || count != 0 {
			t.Fatal(count, err)
		}
		second, err := cat.ReserveIdleSlot(ctx, "other-"+node, slot.Tier, life, false)
		if err != nil || *second != *first {
			t.Fatal(second, err)
		}
		if err := cat.FinishReservation(ctx, life, true); err != nil {
			t.Fatal(err)
		}
		next, err := cat.ReserveIdleSlot(ctx, node, slot.Tier, "next-"+life, false)
		if err != nil || next.Slot != slot {
			t.Fatal(next, err)
		}
		if err := cat.FinishReservation(ctx, "next-"+life, false); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("memory", func(t *testing.T) { check(t, store.NewMemoryRuntimeCatalog()) })
	t.Run("redis", func(t *testing.T) {
		addr := os.Getenv("KONTINUE_TEST_REDIS")
		if addr == "" {
			t.Skip("KONTINUE_TEST_REDIS is unset")
		}
		cat := store.NewRedisRuntimeCatalog(addr, "", 0)
		defer cat.Close()
		check(t, cat)
	})
}

func TestColdCarrierGoesToWaitingLifecycle(t *testing.T) {
	check := func(t *testing.T, cat store.RuntimeCatalog) {
		ctx := context.Background()
		life := fmt.Sprintf("cold-%d", time.Now().UnixNano())
		slot := protocol.CarrierSlot{SlotID: "slot-" + life, NodeID: "node-a", Tier: life}
		if err := cat.ExpectColdCarrier(ctx, life, slot.SlotID, time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.RegisterColdCarrier(ctx, slot, life); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.ReserveIdleSlot(ctx, slot.NodeID, slot.Tier, "other-"+life, false); !errors.Is(err, store.ErrNoIdleCarrierSlot) {
			t.Fatalf("competing lifecycle claimed a dedicated Carrier: %v", err)
		}
		r, err := cat.ReserveIdleSlot(ctx, "node-b", slot.Tier, life, false)
		if err != nil || r.Slot != slot || !r.Cold {
			t.Fatalf("could not replay the dedicated reservation: %+v %v", r, err)
		}
		if err := cat.FinishReservation(ctx, life, false); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.RegisterColdCarrier(ctx, slot, life); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.ClaimIdleSlot(ctx, slot.NodeID, slot.Tier); !errors.Is(err, store.ErrNoIdleCarrierSlot) {
			t.Fatalf("discovery exposed a committed allocation: %v", err)
		}

		cancelled := "cancelled-" + life
		slot.SlotID = "slot-" + cancelled
		if err := cat.ExpectColdCarrier(ctx, cancelled, slot.SlotID, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := cat.FinishReservation(ctx, cancelled, true); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.RegisterColdCarrier(ctx, slot, cancelled); err != nil {
			t.Fatal(err)
		}
		shared, err := cat.ClaimIdleSlot(ctx, slot.NodeID, slot.Tier)
		if err != nil || *shared != slot {
			t.Fatalf("late registration did not return to the shared pool: %+v %v", shared, err)
		}

		early := "network-" + life
		slot.SlotID = "slot-" + early
		slot.NetworkReadyOnly = true
		if published, err := cat.RegisterColdCarrier(ctx, slot, early); err != nil || published {
			t.Fatalf("published a network-only Carrier without a matching expectation: %t %v", published, err)
		}
		if err := cat.ExpectColdCarrier(ctx, early, slot.SlotID, time.Minute); err != nil {
			t.Fatal(err)
		}
		if published, err := cat.RegisterColdCarrier(ctx, slot, early); err != nil || !published {
			t.Fatalf("did not reserve the expected network-only Carrier: %t %v", published, err)
		}
		if err := cat.FinishReservation(ctx, early, true); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.ClaimIdleSlot(ctx, slot.NodeID, slot.Tier); !errors.Is(err, store.ErrNoIdleCarrierSlot) {
			t.Fatalf("cancellation exposed a network-only Carrier to the shared pool: %v", err)
		}
		slot.NetworkReadyOnly = false
		if published, err := cat.RegisterColdCarrier(ctx, slot, early); err != nil || !published {
			t.Fatalf("the completed placeholder could not be discovered after cancellation: %t %v", published, err)
		}
	}
	t.Run("memory", func(t *testing.T) { check(t, store.NewMemoryRuntimeCatalog()) })
	t.Run("redis", func(t *testing.T) {
		addr := os.Getenv("KONTINUE_TEST_REDIS")
		if addr == "" {
			t.Skip("KONTINUE_TEST_REDIS is unset")
		}
		cat := store.NewRedisRuntimeCatalog(addr, "", 0)
		defer cat.Close()
		check(t, cat)
	})
}

func TestIdleCarrierRemovalIsFencedByPodIdentity(t *testing.T) {
	check := func(t *testing.T, cat store.RuntimeCatalog) {
		ctx := context.Background()
		old := protocol.CarrierSlot{SlotID: "slot-" + rand.Text(), NodeID: "node", Tier: rand.Text(), PodUID: "uid-1", PodSandboxID: "cri-1"}
		replacement := old
		replacement.PodUID, replacement.PodSandboxID = "uid-2", "cri-2"
		for _, slot := range []protocol.CarrierSlot{old, replacement} {
			if err := cat.RegisterIdleSlot(ctx, slot); err != nil {
				t.Fatal(err)
			}
		}
		if err := cat.RemoveIdleSlot(ctx, old); err != nil {
			t.Fatal(err)
		}
		// Kubelet can also recreate the CRI sandbox under the same Pod UID.
		oldRuntime := replacement
		oldRuntime.PodSandboxID = "previous-cri"
		if err := cat.RemoveIdleSlot(ctx, oldRuntime); err != nil {
			t.Fatal(err)
		}
		if count, err := cat.CountIdleSlots(ctx, old.NodeID, old.Tier); err != nil || count != 1 {
			t.Fatalf("stale withdrawal removed the replacement: count=%d err=%v", count, err)
		}
		if err := cat.RemoveIdleSlot(ctx, replacement); err != nil {
			t.Fatal(err)
		}
		if _, err := cat.ClaimIdleSlot(ctx, old.NodeID, old.Tier); !errors.Is(err, store.ErrNoIdleCarrierSlot) {
			t.Fatalf("current Carrier is still claimable: %v", err)
		}
	}
	t.Run("memory", func(t *testing.T) { check(t, store.NewMemoryRuntimeCatalog()) })
	t.Run("redis", func(t *testing.T) {
		addr := os.Getenv("KONTINUE_TEST_REDIS")
		if addr == "" {
			t.Skip("KONTINUE_TEST_REDIS is unset")
		}
		cat := store.NewRedisRuntimeCatalog(addr, "", 0)
		defer cat.Close()
		check(t, cat)
	})
}
