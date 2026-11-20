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
