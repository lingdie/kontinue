package apiserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// advanceActivation runs under the sandbox lifecycle lock. Both the exact
// assignment and the target generation must survive an ambiguous node reply.
func (s *Server) advanceActivation(ctx context.Context, sb *store.SandboxRecord) (*store.SandboxRecord, error) {
	if sb.State != store.SandboxStateCreating && sb.State != store.SandboxStateResuming {
		return nil, store.ErrTransitionLocked
	}
	if sb.PendingPlacement == nil {
		request := PlacementRequest{LifecycleID: sb.LifecycleID, Tier: sb.Tier, SnapshotID: sb.ActiveSnapshotID, OriginNodeID: sb.OriginNodeID, AllowFilesystemOnlyFallback: sb.AllowFilesystemOnlyFallback}
		if sb.ActiveSnapshotID != "" {
			snapshot, err := s.cfg.Meta.GetSnapshot(ctx, sb.ActiveSnapshotID)
			if err != nil {
				return nil, err
			}
			request.RequiredFingerprint = &snapshot.Fingerprint
		}
		dec, err := s.cfg.Placement.SelectAndClaimSlot(ctx, request)
		if err != nil {
			return nil, err
		}
		slot := dec.Slot
		slot.NodeID, slot.NodeIP, slot.NodeGRPCAddr = dec.Node.NodeID, dec.Node.NodeIP, dec.Node.GRPCAddr
		sb.PendingPlacement = &store.ActivationPlacement{Slot: slot, RestoreMode: dec.RestoreMode}
		sb.CurrentNodeID, sb.CurrentSlotID = slot.NodeID, slot.SlotID
		sb.CarrierPodNamespace, sb.CarrierPodName = slot.PodNamespace, slot.PodName
		sb.PodIP = slot.PodIP
		sb.PoolAllocation = "WARM_POOL"
		if dec.ColdFallback {
			sb.PoolAllocation = "COLD_FALLBACK"
		}
		if err = s.cfg.Meta.SaveSandbox(ctx, sb); err != nil {
			return nil, err
		}
	}
	if err := s.cfg.Catalog.FinishReservation(ctx, sb.LifecycleID, false); err != nil {
		return nil, err
	}
	p := sb.PendingPlacement
	node, err := s.nodeClient(p.Slot.NodeGRPCAddr)
	if err != nil {
		return nil, err
	}
	req := &protocol.HydrateSlotRequest{Slot: p.Slot, SandboxID: sb.SandboxID, ExecutionID: sb.ExecutionID, LifecycleID: sb.LifecycleID,
		TemplateID: sb.TemplateID, SnapshotID: sb.ActiveSnapshotID, RestoreMode: p.RestoreMode,
		BaseImage: sb.BaseImage, BootstrapToken: sb.BootstrapToken, AccessToken: sb.AccessToken, EnvVars: sb.EnvVars, CPUMillis: sb.CPUMillis, MemoryBytes: sb.MemoryBytes, DeferRoutePublication: true}
	result, err := node.HydrateSlot(ctx, req)
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			sb.State = store.SandboxStateFailed
			sb.FailureReason = status.Convert(err).Message()
			if saveErr := s.cfg.Meta.SaveSandbox(ctx, sb); saveErr != nil {
				return nil, errors.Join(err, saveErr)
			}
		}
		// A lost reply can follow a successful start. Keep the assignment so
		// the next reconciliation contacts the same slot with the same request.
		return nil, err
	}
	if result.ExecutionID != sb.ExecutionID || result.SandboxID != sb.SandboxID || result.PodIP == "" {
		return nil, fmt.Errorf("unexpected activation result for slot %s", p.Slot.SlotID)
	}
	sb.PodIP = result.PodIP
	sb.OriginNodeID = sb.CurrentNodeID
	sb.State = store.SandboxStateRunning
	sb.PendingPlacement = nil
	if err := s.cfg.Meta.SaveSandbox(ctx, sb); err != nil {
		return nil, err
	}
	route := protocol.RouteRecord{NodeID: sb.CurrentNodeID, NodeIP: p.Slot.NodeIP, NodeGRPCAddr: p.Slot.NodeGRPCAddr, SlotID: sb.CurrentSlotID,
		SandboxID: sb.SandboxID, ExecutionID: sb.ExecutionID, LifecycleID: sb.LifecycleID, CarrierPodName: sb.CarrierPodName, PodNamespace: sb.CarrierPodNamespace, PodIP: sb.PodIP, StartedAt: time.Now().UTC()}
	route.OwnerKey = store.OwnerKey(route)
	if err := s.cfg.Catalog.PublishRoute(ctx, route); err != nil {
		return nil, err
	}
	_ = s.cfg.Catalog.TouchSandboxActive(ctx, sb.SandboxID, time.Now().UTC())
	return sb, nil
}

// The sweep's observation may predate a suspend or delete. Recheck under
// the lock rather than issuing a fresh user-requested resume.
func (s *Server) resumePending(ctx context.Context, observed *store.SandboxRecord) (*store.SandboxRecord, error) {
	unlock, err := s.lockExecution(ctx, observed.SandboxID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	current, err := s.cfg.Meta.GetSandbox(ctx, observed.SandboxID)
	if err != nil {
		return nil, err
	}
	if current.State != observed.State || current.ExecutionID != observed.ExecutionID || current.LifecycleID != observed.LifecycleID {
		return nil, store.ErrStaleExecutionFence
	}
	return s.advanceActivation(ctx, current)
}
