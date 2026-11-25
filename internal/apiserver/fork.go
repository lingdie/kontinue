package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

type forkChildResult struct {
	ChildID string
	Sandbox *store.SandboxRecord
	Err     error
}

// ForkSandbox persists the capture and branch identities before contacting a node.
func (s *Server) ForkSandbox(ctx context.Context, sourceID string, req ForkSandboxRequest) (_ []*store.SandboxRecord, retErr error) {
	results, err := s.ForkSandboxWithResults(ctx, sourceID, req)
	if err != nil {
		return nil, err
	}
	children := make([]*store.SandboxRecord, 0, len(results))
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		children = append(children, result.Sandbox)
	}
	return children, nil
}

func (s *Server) ForkSandboxWithResults(ctx context.Context, sourceID string, req ForkSandboxRequest) ([]forkChildResult, error) {
	if len(req.Children) > 0 {
		if req.Count > 0 && req.Count != len(req.Children) {
			return nil, fmt.Errorf("fork count must match children length")
		}
		req.Count = len(req.Children)
	} else if req.Count <= 0 {
		req.Count = 1
	}
	for _, child := range req.Children {
		if err := validateForkChildConfig(sourceID, child); err != nil {
			return nil, err
		}
	}
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.fork_sandbox", attribute.String(telemetry.AttrSandboxID, sourceID), attribute.Int("fork.count", req.Count))
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()
	receipt, err := s.prepareFork(ctx, sourceID, &req, "")
	if err != nil {
		retErr = err
		return nil, err
	}
	results := make([]forkChildResult, len(receipt.ChildIDs))
	var workers errgroup.Group
	for i, id := range receipt.ChildIDs {
		i, id := i, id
		workers.Go(func() error {
			result := forkChildResult{ChildID: id}
			lock, err := s.lockActivation(ctx, id)
			if err != nil {
				result.Err = err
				results[i] = result
				return nil
			}
			defer lock.close()
			child, err := s.sandbox(ctx, id)
			if err != nil {
				result.Err = err
				results[i] = result
				return nil
			}
			// The caller might retry after a branch was deleted and its name reused.
			if child.ForkOperationID != receipt.OperationID {
				result.Err = store.ErrStaleExecutionFence
				results[i] = result
				return nil
			}
			if child.State == store.SandboxStateCreating {
				child, err = s.advanceActivation(ctx, child, lock)
				if err != nil {
					result.Err = err
					results[i] = result
					return nil
				}
			}
			result.Sandbox = child
			results[i] = result
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
			fork = &store.PendingFork{ForkReceipt: store.ForkReceipt{OperationID: randHex(12), RequestHash: digest}, RequestKey: key, SnapshotID: snapshotID, EnvVars: req.EnvVars, Children: append([]store.ForkChildConfig(nil), req.Children...)}
			for i := 0; i < req.Count; i++ {
				childID := ""
				if i < len(req.Children) {
					childID = req.Children[i].SandboxID
				}
				if childID == "" {
					childID = fmt.Sprintf("%s-fork-%d-%s", sourceID, i+1, randHex(8))
				}
				for _, existing := range fork.ChildIDs {
					if existing == childID {
						return nil, fmt.Errorf("duplicate fork child sandbox id %q", childID)
					}
				}
				fork.ChildIDs = append(fork.ChildIDs, childID)
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
		snapshot, err = s.captureSandbox(ctx, src, route, fork.SnapshotID, protocol.SnapshotKindLiveFork, checkpointCaptureIfMissing)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	children := make([]*store.SandboxRecord, 0, len(fork.ChildIDs))
	for i, id := range fork.ChildIDs {
		var config store.ForkChildConfig
		if i < len(fork.Children) {
			config = fork.Children[i]
		}
		env := make(map[string]string, len(src.EnvVars)+len(fork.EnvVars)+len(config.EnvVars))
		maps.Copy(env, src.EnvVars)
		maps.Copy(env, fork.EnvVars)
		maps.Copy(env, config.EnvVars)
		metadata := maps.Clone(src.Metadata)
		if metadata == nil && len(config.Metadata) > 0 {
			metadata = make(map[string]string, len(config.Metadata))
		}
		maps.Copy(metadata, config.Metadata)
		idleTimeout := src.IdleTimeoutSeconds
		if config.IdleTimeoutSeconds != nil {
			idleTimeout = *config.IdleTimeoutSeconds
		}
		cpuMillis := src.CPUMillis
		if config.CPUMillis != nil {
			cpuMillis = *config.CPUMillis
		}
		memoryBytes := src.MemoryBytes
		if config.MemoryBytes != nil {
			memoryBytes = *config.MemoryBytes
		}
		volumes := slices.Clone(src.Volumes)
		if config.Volumes != nil {
			volumes = slices.Clone(config.Volumes)
		}
		children = append(children, &store.SandboxRecord{
			TenantID: src.TenantID, SandboxID: id, ForkOperationID: fork.OperationID, TemplateID: src.TemplateID, BaseImage: src.BaseImage, Tier: src.Tier,
			State: store.SandboxStateCreating, ExecutionID: src.ExecutionID + 1, LifecycleID: "life-" + randHex(6),
			OriginNodeID: snapshot.OriginNodeID, ActiveSnapshotID: snapshot.SnapshotID, BootstrapToken: snapshot.BootstrapToken,
			AccessToken: "kt_fork_" + randHex(16), EnvVars: env, CPUMillis: cpuMillis, MemoryBytes: memoryBytes,
			IdleTimeoutSeconds: idleTimeout, AllowFilesystemOnlyFallback: src.AllowFilesystemOnlyFallback,
			Metadata: metadata,
			Volumes:  volumes,
		})
	}
	if err := s.cfg.Meta.CommitFork(ctx, src, children); err != nil {
		return nil, err
	}
	return &fork.ForkReceipt, nil
}

func validateForkChildConfig(sourceID string, child store.ForkChildConfig) error {
	if child.SandboxID != "" && !validForkSandboxID(child.SandboxID, sourceID) {
		return fmt.Errorf("invalid fork child sandbox id %q", child.SandboxID)
	}
	if child.IdleTimeoutSeconds != nil && *child.IdleTimeoutSeconds < 0 {
		return fmt.Errorf("fork child idle timeout cannot be negative")
	}
	if child.CPUMillis != nil && *child.CPUMillis < 0 {
		return fmt.Errorf("fork child cpu millis cannot be negative")
	}
	if child.MemoryBytes != nil && *child.MemoryBytes < 0 {
		return fmt.Errorf("fork child memory bytes cannot be negative")
	}
	for _, volume := range child.Volumes {
		if volume.VolumeID == "" || filepath.Base(volume.VolumeID) != volume.VolumeID || volume.MountPath == "" || !filepath.IsAbs(volume.MountPath) {
			return fmt.Errorf("fork child volume mounts require a volumeId and absolute mountPath")
		}
	}
	return nil
}

func validForkSandboxID(id, sourceID string) bool {
	return id != sourceID && len(id) <= 128 && strings.TrimSpace(id) == id && id != "." && id != ".." && !strings.ContainsAny(id, "/\\")
}
