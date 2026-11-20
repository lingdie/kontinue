package apiserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errActivationAdvanced = errors.New("execution advanced while waiting for its Carrier")

type activationLock struct {
	release func()
}

func (l *activationLock) close() {
	if l.release != nil {
		l.release()
		l.release = nil
	}
}

func (s *Server) lockActivation(ctx context.Context, sandboxID string) (*activationLock, error) {
	release, err := s.lockExecution(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	return &activationLock{release: release}, nil
}

// A concurrent waiter or the reconciler can process the same readiness
// notification first. Wait for its lock while respecting the Carrier deadline.
func (s *Server) relockActivation(ctx context.Context, id string, lock *activationLock) (retErr error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.reacquire_activation_lock")
	defer func() { telemetry.EndSpan(span, retErr) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		release, err := s.lockExecution(ctx, id)
		if !errors.Is(err, store.ErrTransitionLocked) {
			if err == nil {
				lock.release = release
			}
			return err
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Hold the lock when writing metadata, claiming a shared slot or activating a
// guest. While waiting for a cold Pod, discovery can publish only its fenced
// reservation and delete remains free to cancel the expectation.
func (s *Server) advanceActivation(ctx context.Context, sb *store.SandboxRecord, lock *activationLock) (*store.SandboxRecord, error) {
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
		wait := func(waitCtx context.Context, ready func(context.Context) error) error {
			lock.close()
			if err := ready(waitCtx); err != nil {
				return err
			}
			if err := s.relockActivation(waitCtx, sb.SandboxID, lock); err != nil {
				return err
			}
			current, err := s.cfg.Meta.GetSandbox(waitCtx, sb.SandboxID)
			if errors.Is(err, store.ErrNotFound) {
				return store.ErrStaleExecutionFence
			}
			if err != nil {
				return err
			}
			if current.ExecutionID != sb.ExecutionID || current.LifecycleID != sb.LifecycleID {
				return store.ErrStaleExecutionFence
			}
			if current.State != sb.State && current.State != store.SandboxStateRunning {
				return store.ErrTransitionLocked
			}
			sb = current
			if sb.PendingPlacement != nil || sb.State == store.SandboxStateRunning {
				return errActivationAdvanced
			}
			return nil
		}
		dec, err := s.cfg.Placement.selectAndClaimSlot(ctx, request, wait)
		if errors.Is(err, errActivationAdvanced) {
			if sb.State == store.SandboxStateRunning {
				return sb, nil
			}
		} else if err != nil {
			return nil, err
		} else {
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
	}
	if err := s.cfg.Catalog.FinishReservation(ctx, sb.LifecycleID, false); err != nil {
		return nil, err
	}
	p := sb.PendingPlacement
	node, err := s.nodeClient(ctx, p.Slot.NodeGRPCAddr)
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
	sb.ArenaTier = result.ArenaTier
	sb.RestoreViewCopiedBytes = result.RestoreViewCopiedBytes
	sb.OriginNodeID = sb.CurrentNodeID
	sb.State = store.SandboxStateRunning
	sb.PendingPlacement = nil
	if err := s.cfg.Meta.SaveSandbox(ctx, sb); err != nil {
		return nil, err
	}
	route := protocol.RouteRecord{NodeID: sb.CurrentNodeID, NodeIP: p.Slot.NodeIP, NodeGRPCAddr: p.Slot.NodeGRPCAddr, SlotID: sb.CurrentSlotID,
		SandboxID: sb.SandboxID, ExecutionID: sb.ExecutionID, LifecycleID: sb.LifecycleID, CarrierPodName: sb.CarrierPodName, PodNamespace: sb.CarrierPodNamespace, PodIP: sb.PodIP, StartedAt: time.Now().UTC()}
	route.AccessDigest = protocol.AccessDigest(sb.AccessToken)
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
	lock, err := s.lockActivation(ctx, observed.SandboxID)
	if err != nil {
		return nil, err
	}
	defer lock.close()
	current, err := s.cfg.Meta.GetSandbox(ctx, observed.SandboxID)
	if err != nil {
		return nil, err
	}
	if current.State != observed.State || current.ExecutionID != observed.ExecutionID || current.LifecycleID != observed.LifecycleID {
		return nil, store.ErrStaleExecutionFence
	}
	return s.advanceActivation(ctx, current, lock)
}
