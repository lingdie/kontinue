package hydrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

const lost ownershipPhase = "lost"

var errInstanceLost = errors.New("runsc instance is unavailable")
var errWrongOwner = errors.New("runsc instance identity does not match")

func validInstanceID(id string) bool {
	return strings.HasPrefix(id, "kt-") && filepath.Base(id) == id
}

type runtimeInspector interface {
	inspectInstance(context.Context, ownership) error
}

type bundleIdentity struct {
	ContainerID, SlotID, PodUID, SandboxID, LifecycleID, ViewPath string
	ExecutionID                                                   int64
}

func (d *GVisorSandboxDriver) inspectInstance(ctx context.Context, o ownership) error {
	root := "--root=" + d.cfg.RunscStateRoot
	bundle := filepath.Join(d.cfg.BundlesDir, o.ContainerID)
	// A successful list distinguishes a missing ID from a failed runtime query.
	out, err := d.hostCommand(ctx, d.cfg.RunscWrapperBin, root, "list", "--quiet").Output()
	if err != nil {
		return fmt.Errorf("list runsc instances: %w", err)
	}
	found := false
	for _, id := range strings.Fields(string(out)) {
		if id == o.ContainerID {
			found = true
			break
		}
	}
	var actual bundleIdentity
	data, err := os.ReadFile(filepath.Join(bundle, "owner.json"))
	if !found && errors.Is(err, os.ErrNotExist) {
		if _, e := os.Lstat(bundle); errors.Is(e, os.ErrNotExist) {
			return errInstanceLost
		}
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &actual); err != nil {
		return err
	}
	expected := bundleIdentity{ContainerID: o.ContainerID, SlotID: o.Slot.SlotID, PodUID: o.Slot.PodUID, SandboxID: o.SandboxID, ExecutionID: o.ExecutionID, LifecycleID: o.LifecycleID, ViewPath: o.ViewPath}
	if expected != actual {
		return errWrongOwner
	}

	if !found {
		return errInstanceLost
	}
	out, err = d.hostCommand(ctx, d.cfg.RunscWrapperBin, root, "state", o.ContainerID).Output()
	if err != nil {
		return fmt.Errorf("inspect runsc state: %w", err)
	}
	var state struct {
		ID, Status, Bundle string
		Pid                int
	}
	if err := json.Unmarshal(out, &state); err != nil {
		return err
	}
	if state.ID != o.ContainerID || state.Bundle != bundle {
		return errWrongOwner
	}
	if state.Status == "stopped" {
		return errInstanceLost
	}
	if state.Status != "running" || state.Pid <= 0 {
		return fmt.Errorf("runsc state %s", state.Status)
	}
	return nil
}

// Recover reserves every durable claim before discovery or RPCs start. Temporary
// inspection failures stay reserved and are retried without mutating the guest.
func (s *Service) Recover(ctx context.Context) error {
	claims, err := s.journal.load(s.cfg.NodeID)
	if err != nil {
		return err
	}
	var pending []*activeSlotState
	for _, o := range claims {
		st := &activeSlotState{ownership: o, restored: true}
		s.activeSlots[o.Slot.SlotID] = st
		pending = append(pending, st)
	}
	retry := func() {
		var remaining []*activeSlotState
		for _, st := range pending {
			attempt, cancel := context.WithTimeout(ctx, 4*time.Second)
			err := s.reconcile(attempt, st)
			cancel()
			if err != nil {
				log.Printf("reconcile slot %s: %v", st.Slot.SlotID, err)
				remaining = append(remaining, st)
			}
		}
		pending = remaining
	}
	retry()
	if len(pending) > 0 {
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for len(pending) > 0 {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					retry()
				}
			}
		}()
	}
	return nil
}

func (s *Service) reconcile(ctx context.Context, st *activeSlotState) error {
	st.releaseMu.Lock()
	defer st.releaseMu.Unlock()
	s.mu.Lock()
	current := s.activeSlots[st.Slot.SlotID] == st
	s.mu.Unlock()
	if !current {
		return nil
	}
	if st.Phase == lost || st.Phase == releasing {
		return nil
	}
	inspector, ok := s.cfg.Driver.(runtimeInspector)
	if !ok {
		return errors.New("driver does not support ownership inspection")
	}
	err := inspector.inspectInstance(ctx, st.ownership)
	if err == nil && st.ViewID != "" {
		err = s.cfg.Arena.AdoptView(st.ViewID, st.ViewPath, st.View)
	}
	if err == nil && st.Phase != activeExecution {
		err = errors.New("incomplete hydration")
	}
	if err == nil {
		var h *protocol.HealthResponse
		h, err = s.cfg.KtdClient.Health(ctx, st.KtdEndpoint, "")
		if err == nil && (h.SandboxID != st.SandboxID || h.ExecutionID != st.ExecutionID) {
			err = errWrongOwner
		}
		if err == nil && (!h.Initialized || !h.Healthy) {
			err = errors.New("guest is not ready")
		}
	}
	if errors.Is(err, errInstanceLost) || errors.Is(err, errWrongOwner) || errors.Is(err, arena.ErrViewLost) {
		next := st.ownership
		next.Phase = lost
		next.FailureReason = err.Error()
		if e := s.journal.write(next, false); e != nil {
			return e
		}
		st.Phase, st.FailureReason = next.Phase, next.FailureReason
		return nil
	}
	if err != nil {
		return err
	}
	if err = s.mapSlot(ctx, st); err != nil {
		return err
	}
	s.mu.Lock()
	s.byContainer[st.ContainerID] = st
	s.mu.Unlock()
	log.Printf("adopted slot %s sandbox %s generation %d runsc %s", st.Slot.SlotID, st.SandboxID, st.ExecutionID, st.ContainerID)
	return nil
}
