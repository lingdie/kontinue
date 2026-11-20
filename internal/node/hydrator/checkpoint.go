package hydrator

import (
	"context"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func matchesCheckpointOwner(st *activeSlotState, req *protocol.PrepareCheckpointRequest) bool {
	return st.Slot.SlotID == req.SlotID && st.SandboxID == req.SandboxID &&
		(req.ExecutionID == 0 || req.ExecutionID == st.ExecutionID) &&
		(req.LifecycleID == "" || req.LifecycleID == st.LifecycleID)
}

func (s *Service) recoverCheckpointResult(ctx context.Context, req *protocol.PrepareCheckpointRequest) (*checkpointReceipt, error) {
	s.mu.Lock()
	st := s.activeSlots[req.SlotID]
	s.mu.Unlock()
	if st == nil {
		return nil, nil
	}
	st.releaseMu.Lock()
	defer st.releaseMu.Unlock()
	s.mu.Lock()
	current := s.activeSlots[req.SlotID] == st
	s.mu.Unlock()
	if !current || !matchesCheckpointOwner(st, req) {
		return nil, store.ErrStaleExecutionFence
	}
	return s.replayCheckpoint(ctx, st, req)
}

// replayCheckpoint and commitCheckpoint run under the owning slot's releaseMu.
func (s *Service) replayCheckpoint(ctx context.Context, st *activeSlotState, req *protocol.PrepareCheckpointRequest) (*checkpointReceipt, error) {
	if saved := st.LastCheckpoint; saved != nil && saved.Result.Manifest.SnapshotID == req.TargetSnapshotID {
		if saved.RequestHash != checkpointRequestHash(req) {
			return nil, store.ErrStaleExecutionFence
		}
		return saved, nil
	}
	staged := st.StagedCheckpoint
	if staged == nil || staged.Result.Manifest.SnapshotID != req.TargetSnapshotID {
		return nil, nil
	}
	if staged.RequestHash != checkpointRequestHash(req) {
		return nil, store.ErrStaleExecutionFence
	}
	if err := s.cfg.Arena.RecoverCheckpoint(ctx, &staged.Result.Manifest, staged.Result.ManifestURI); err != nil {
		return nil, err
	}
	return s.commitCheckpoint(st)
}

func (s *Service) commitCheckpoint(st *activeSlotState) (*checkpointReceipt, error) {
	next := st.ownership
	next.LastCheckpoint, next.StagedCheckpoint = st.StagedCheckpoint, nil
	if err := s.journal.write(next, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if previous := st.LastCheckpoint; previous != nil && s.results[previous.Result.Manifest.SnapshotID] == previous {
		delete(s.results, previous.Result.Manifest.SnapshotID)
	}
	st.LastCheckpoint, st.StagedCheckpoint = next.LastCheckpoint, nil
	s.results[next.LastCheckpoint.Result.Manifest.SnapshotID] = next.LastCheckpoint
	s.mu.Unlock()
	return next.LastCheckpoint, nil
}
