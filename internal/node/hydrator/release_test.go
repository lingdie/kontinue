package hydrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type recordingDriver struct {
	RuntimeDriver
	stopped []string
}

func (d *recordingDriver) StopSandboxContainer(_ context.Context, _ protocol.CarrierSlot, id string) error {
	d.stopped = append(d.stopped, id)
	return nil
}

func TestReleaseOnlyStopsTheClaimedExecution(t *testing.T) {
	ctx := context.Background()
	routes := store.NewMemoryRuntimeCatalog()
	d := &recordingDriver{}
	s := NewService(ServiceConfig{Driver: d, Routes: routes, ScratchDir: t.TempDir()})
	live := &activeSlotState{ownership: ownership{SandboxID: "session", ExecutionID: 2, Slot: protocol.CarrierSlot{SlotID: "current"}, ContainerID: "live"}}
	s.activeSlots["current"] = live
	s.byContainer["live"] = live
	if err := routes.PublishRoute(ctx, protocol.RouteRecord{SandboxID: "session", SlotID: "current", ExecutionID: 2}); err != nil {
		t.Fatal(err)
	}
	for _, req := range []protocol.ReleaseSlotRequest{
		{SandboxID: "session", SlotID: "previous", ExecutionID: 1},
		{SandboxID: "session", SlotID: "current", ExecutionID: 1},
		{SandboxID: "session", SlotID: "previous", ExecutionID: 2},
		{SandboxID: "another", SlotID: "current", ExecutionID: 2},
	} {
		res, err := s.ReleaseSlot(ctx, &req)
		if err != nil || res.Released || len(d.stopped) != 0 {
			t.Fatalf("stale release %+v: response=%+v stops=%v err=%v", req, res, d.stopped, err)
		}
		if _, err := routes.GetRoute(ctx, "session"); err != nil {
			t.Fatal(err)
		}
	}
	req := protocol.ReleaseSlotRequest{SandboxID: "session", SlotID: "current", ExecutionID: 2}
	for range 2 {
		if _, err := s.ReleaseSlot(ctx, &req); err != nil {
			t.Fatal(err)
		}
	}
	if len(d.stopped) != 1 || s.ActiveCount() != 0 {
		t.Fatalf("stops=%v active=%d", d.stopped, s.ActiveCount())
	}
}

func TestReleaseWithoutClaimUsesTheFencedPodIdentity(t *testing.T) {
	s := NewService(ServiceConfig{ScratchDir: t.TempDir()})
	response, err := s.ReleaseSlot(context.Background(), &protocol.ReleaseSlotRequest{
		SlotID: "slot", SandboxID: "box", PodUID: "pod", ExecutionID: 1, LifecycleID: "life",
	})
	if err != nil || response == nil || !response.Released || response.PodUID != "pod" {
		t.Fatalf("unclaimed release was not idempotent: response=%+v err=%v", response, err)
	}
}

func TestStopKeepsBundleWhenRuntimeDeleteFails(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "runtime")
	bundle := filepath.Join(dir, "bundles", "kt-test")
	if err := os.MkdirAll(bundle, 0700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nexit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	d := &GVisorSandboxDriver{cfg: GVisorDriverConfig{RunscWrapperBin: bin, RunscStateRoot: dir, BundlesDir: filepath.Dir(bundle)}}
	if err := d.StopSandboxContainer(context.Background(), protocol.CarrierSlot{}, "kt-test"); err == nil {
		t.Fatal("reported success while container is present")
	}
	if _, err := os.Stat(bundle); err != nil {
		t.Fatal("lost runtime bundle", err)
	}
}

func TestReleaseReceiptSurvivesRestartAndDistinguishesUnknownLife(t *testing.T) {
	ctx := context.Background()
	d := &recordingDriver{}
	cfg := ServiceConfig{Driver: d, ScratchDir: t.TempDir()}
	s := NewService(cfg)
	o := ownership{Slot: protocol.CarrierSlot{SlotID: "slot", PodUID: "pod-uid"}, SandboxID: "box", ExecutionID: 1, LifecycleID: "life", ContainerID: "kt-one", Phase: activeExecution}
	st := &activeSlotState{ownership: o}
	s.activeSlots["slot"] = st
	s.byContainer["kt-one"] = st
	req := protocol.ReleaseSlotRequest{SlotID: "slot", SandboxID: "box", ExecutionID: 1, LifecycleID: "life"}
	for _, node := range []*Service{s, NewService(cfg)} {
		resp, err := node.ReleaseSlot(ctx, &req)
		if err != nil || !resp.Released || resp.PodUID != "pod-uid" || len(d.stopped) != 1 {
			t.Fatalf("response=%v err=%v stops=%v", resp, err, d.stopped)
		}
	}
	req.LifecycleID = "another"
	resp, err := s.ReleaseSlot(ctx, &req)
	if err != nil || resp.Released {
		t.Fatalf("unconfirmed release %v %v", resp, err)
	}
}

func TestReleaseRetryFinishesClaimAfterReceiptWasPersisted(t *testing.T) {
	ctx := context.Background()
	driver := &inspectingDriver{inspectErr: errInstanceLost}
	cfg := ServiceConfig{Driver: driver, ScratchDir: t.TempDir(), NodeID: "node"}
	previous := NewService(cfg)
	req := protocol.ReleaseSlotRequest{SlotID: "slot", SandboxID: "box", ExecutionID: 1, LifecycleID: "old-life"}
	claim := ownership{
		Version: 1, Phase: releasing, ContainerID: "kt-original",
		Slot:      protocol.CarrierSlot{SlotID: req.SlotID, NodeID: cfg.NodeID, PodUID: "original-pod"},
		SandboxID: req.SandboxID, ExecutionID: req.ExecutionID, LifecycleID: req.LifecycleID,
	}
	receipt := protocol.ReleaseSlotResponse{Released: true, Tainted: true, PodUID: claim.Slot.PodUID}
	if err := previous.journal.write(claim, true); err != nil {
		t.Fatal(err)
	}
	if err := previous.journal.persist(previous.journal.receipt(&req), receipt, false); err != nil {
		t.Fatal(err)
	}

	restarted := NewService(cfg)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	req.ReuseSlot = true // Replay the original response even if the retry changes this flag.
	response, err := restarted.ReleaseSlot(ctx, &req)
	if err != nil || response == nil || *response != receipt || len(driver.stopped) != 0 || restarted.ActiveCount() != 0 {
		t.Fatalf("retry after restart: response=%v err=%v stops=%v active=%d", response, err, driver.stopped, restarted.ActiveCount())
	}

	// A new lifecycle can reuse the same slot and execution number after deletion.
	replacement := claim
	replacement.LifecycleID = "new-life"
	replacement.Slot.PodUID = "replacement-pod"
	replacement.ContainerID = "kt-replacement"
	replacement.Phase = activeExecution
	if err := restarted.journal.write(replacement, true); err != nil {
		t.Fatalf("old claim still on disk: %v", err)
	}
	current := &activeSlotState{ownership: replacement}
	restarted.activeSlots[req.SlotID] = current
	restarted.byContainer[replacement.ContainerID] = current
	response, err = restarted.ReleaseSlot(ctx, &req)
	if err != nil || response == nil || *response != receipt || len(driver.stopped) != 0 {
		t.Fatalf("retry with reused slot: response=%v err=%v stops=%v", response, err, driver.stopped)
	}
	claims, err := restarted.journal.load(cfg.NodeID)
	if err != nil || len(claims) != 1 || claims[0] != replacement || restarted.activeSlots[req.SlotID] != current || restarted.byContainer[replacement.ContainerID] != current {
		t.Fatalf("replacement claim lost: claims=%v err=%v", claims, err)
	}
}

func TestRecoverFinishesReleaseClaimWithoutReceipt(t *testing.T) {
	ctx := context.Background()
	driver := &recordingDriver{}
	routes := store.NewMemoryRuntimeCatalog()
	cfg := ServiceConfig{Driver: driver, Routes: routes, ScratchDir: t.TempDir(), NodeID: "node"}
	previous := NewService(cfg)
	claim := ownership{
		Version: 1, Phase: releasing, ReleaseTainted: true, ContainerID: "kt-original",
		Slot:      protocol.CarrierSlot{SlotID: "slot", NodeID: cfg.NodeID, PodUID: "pod"},
		SandboxID: "box", ExecutionID: 1, LifecycleID: "life",
	}
	if err := previous.journal.write(claim, true); err != nil {
		t.Fatal(err)
	}

	restarted := NewService(cfg)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if len(driver.stopped) != 1 || driver.stopped[0] != claim.ContainerID {
		t.Fatalf("recovery did not finish the old release: stops=%v", driver.stopped)
	}
	if claims, err := restarted.journal.load(cfg.NodeID); err != nil || len(claims) != 0 {
		t.Fatalf("release claim remained after recovery: claims=%v err=%v", claims, err)
	}
	req := &protocol.ReleaseSlotRequest{SlotID: claim.Slot.SlotID, SandboxID: claim.SandboxID, ExecutionID: claim.ExecutionID, LifecycleID: claim.LifecycleID}
	receipt, err := restarted.journal.previousRelease(req)
	if err != nil || !receipt.Released || receipt.PodUID != claim.Slot.PodUID || !receipt.Tainted {
		t.Fatalf("recovery did not persist release receipt: receipt=%+v err=%v", receipt, err)
	}
	report, err := routes.ExecutionReport(ctx, claim.route(cfg))
	if err != nil || report.State != store.OwnerFailed {
		t.Fatalf("recovery did not publish terminal release signal: report=%+v err=%v", report, err)
	}
}
