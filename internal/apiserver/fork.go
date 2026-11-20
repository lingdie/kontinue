package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

// ForkSandbox persists the capture and branch identities before contacting a node.
func (s *Server) ForkSandbox(ctx context.Context, sourceID string, req ForkSandboxRequest) (_ []*store.SandboxRecord, retErr error) {
	if req.Count <= 0 {
		req.Count = 1
	}
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.fork_sandbox", attribute.String(telemetry.AttrSandboxID, sourceID), attribute.Int("fork.count", req.Count))
	defer func() { telemetry.EndSpan(span, retErr) }()
	receipt, err := s.prepareFork(ctx, sourceID, &req, "")
	if err != nil {
		return nil, err
	}
	results := make([]*store.SandboxRecord, len(receipt.ChildIDs))
	var workers errgroup.Group
	for i, id := range receipt.ChildIDs {
		workers.Go(func() error {
			lock, err := s.lockActivation(ctx, id)
			if err != nil {
				return err
			}
			defer lock.close()
			child, err := s.sandbox(ctx, id)
			if err != nil {
				return err
			}
			// The caller might retry after a branch was deleted and its name reused.
			if child.ForkOperationID != receipt.OperationID {
				return store.ErrStaleExecutionFence
			}
			if child.State == store.SandboxStateCreating {
				child, err = s.advanceActivation(ctx, child, lock)
				if err != nil {
					return err
				}
			}
			results[i] = child
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// A sweeper supplies only the observed operation ID. It cannot start a new fork.
func (s *Server) prepareFork(ctx context.Context, sourceID string, req *ForkSandboxRequest, observedID string) (*store.ForkReceipt, error) {
	unlock, err := s.lockExecution(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	src, err := s.sandbox(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	fork := src.PendingFork
	if req == nil {
		if fork == nil || fork.OperationID != observedID {
			return nil, store.ErrStaleExecutionFence
		}
	} else {
		raw, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(raw))
		key := ""
		if req.RequestKey != "" {
			key = fmt.Sprintf("%x", sha256.Sum256([]byte(req.RequestKey)))
			receipt, err := s.cfg.Meta.GetForkReceipt(ctx, sourceID, key)
			if err == nil {
				if receipt.RequestHash != digest {
					return nil, fmt.Errorf("idempotency key already used with different fork options: %w", store.ErrTransitionLocked)
				}
				return receipt, nil
			}
			if !errors.Is(err, store.ErrNotFound) {
				return nil, err
			}
		}
		if fork != nil && (fork.RequestKey != key || fork.RequestHash != digest) {
			return nil, store.ErrTransitionLocked
		}
		if fork == nil {
			snapshotID := src.ActiveSnapshotID
			switch src.State {
			case store.SandboxStateRunning:
				snapshotID = "snap-fork-" + randHex(12)
			case store.SandboxStateSuspended:
				if snapshotID == "" {
					return nil, store.ErrNotFound
				}
			default:
				return nil, store.ErrTransitionLocked
			}
			fork = &store.PendingFork{ForkReceipt: store.ForkReceipt{OperationID: randHex(12), RequestHash: digest}, RequestKey: key, SnapshotID: snapshotID, EnvVars: req.EnvVars}
			for i := 0; i < req.Count; i++ {
				fork.ChildIDs = append(fork.ChildIDs, fmt.Sprintf("%s-fork-%d-%s", sourceID, i+1, randHex(8)))
			}
			src.PendingFork = fork
			if err := s.cfg.Meta.SaveSandbox(ctx, src); err != nil {
				return nil, err
			}
		}
	}
	snapshot, err := s.cfg.Meta.GetSnapshot(ctx, fork.SnapshotID)
	if errors.Is(err, store.ErrNotFound) && src.State == store.SandboxStateRunning {
		route, err := s.routeForExecution(ctx, src)
		if err != nil {
			return nil, err
		}
		snapshot, err = s.captureSandbox(ctx, src, route, fork.SnapshotID, protocol.SnapshotKindLiveFork)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	env := make(map[string]string, len(src.EnvVars)+len(fork.EnvVars))
	maps.Copy(env, src.EnvVars)
	maps.Copy(env, fork.EnvVars)
	children := make([]*store.SandboxRecord, 0, len(fork.ChildIDs))
	for _, id := range fork.ChildIDs {
		children = append(children, &store.SandboxRecord{
			TenantID: src.TenantID, SandboxID: id, ForkOperationID: fork.OperationID, TemplateID: src.TemplateID, BaseImage: src.BaseImage, Tier: src.Tier,
			State: store.SandboxStateCreating, ExecutionID: src.ExecutionID + 1, LifecycleID: "life-" + randHex(6),
			OriginNodeID: snapshot.OriginNodeID, ActiveSnapshotID: snapshot.SnapshotID, BootstrapToken: snapshot.BootstrapToken,
			AccessToken: "kt_fork_" + randHex(16), EnvVars: env, CPUMillis: src.CPUMillis, MemoryBytes: src.MemoryBytes,
			IdleTimeoutSeconds: src.IdleTimeoutSeconds, AllowFilesystemOnlyFallback: src.AllowFilesystemOnlyFallback,
		})
	}
	if err := s.cfg.Meta.CommitFork(ctx, src, children); err != nil {
		return nil, err
	}
	return &fork.ForkReceipt, nil
}
