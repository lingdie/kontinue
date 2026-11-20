package hydrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/internal/crishim"
	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// RuntimeDriver abstracts container swap & gVisor checkpoint operations inside a Carrier PodSandbox.
// Production uses CRIRuntimeDriver (containerd + kontinue-runsc); integration tests can plug LocalKtdDriver.
type RuntimeDriver interface {
	SwapAndStartSandboxContainer(ctx context.Context, opts SwapContainerOptions) (newContainerID string, ktdEndpoint string, err error)
	CheckpointSandboxContainer(ctx context.Context, containerID, rawOutputDir string, leaveRunning bool) error
	StopSandboxContainer(ctx context.Context, slot protocol.CarrierSlot, activeContainerID string) error
}

type SwapContainerOptions struct {
	ContainerID    string
	ExecutionID    int64
	LifecycleID    string
	Slot           protocol.CarrierSlot
	SandboxID      string
	BaseImage      string
	RestoreViewDir string
	RestoreMode    string
	BootstrapToken string
	AccessToken    string
	CPUMillis      int64
	MemoryBytes    int64
}

// RoutePublisher updates Redis route records with ExecutionID Lua fencing when a slot goes live.
type RoutePublisher interface {
	BeginNodeRecovery(context.Context, string) error
	ReportExecution(context.Context, protocol.RouteRecord, store.ExecutionReport) error
	PublishRoute(ctx context.Context, route protocol.RouteRecord) error
}

type activeSlotState struct {
	restored         bool
	releaseMu        sync.Mutex
	recoveryDeadline time.Time
	ownership
}

type ServiceConfig struct {
	Carriers     runtimeapi.RuntimeServiceClient
	NodeID       string
	NodeIP       string
	NodeGRPCAddr string
	ScratchDir   string
	ShimAdminURL string
	Fingerprint  protocol.CompatibilityFingerprint
	Arena        *arena.Manager
	Driver       RuntimeDriver
	Routes       RoutePublisher
	KtdClient    *protocol.KtdClient
	// CarrierWatcher is optional; when set, ResyncIdleCarriers forces an immediate idle-pool sync.
	CarrierWatcher *CarrierPodWatcher
}

// Service implements protocol.NodeControlServer on each worker node (kontinued).
type Service struct {
	cfg     ServiceConfig
	journal ownershipJournal

	mu          sync.Mutex
	pendingCkpt map[string]*protocol.PrepareCheckpointRequest // keyed by verified runtime container ID
	activeSlots map[string]*activeSlotState                   // keyed by slotID
	byContainer map[string]*activeSlotState                   // keyed by activeContainerID
	results     map[string]*checkpointReceipt
}

func NewService(cfg ServiceConfig) *Service {
	if cfg.KtdClient == nil {
		cfg.KtdClient = protocol.NewKtdClient(15 * time.Second)
	}
	if cfg.ScratchDir == "" {
		cfg.ScratchDir = filepath.Join(os.TempDir(), "kontinued-scratch")
	}
	_ = os.MkdirAll(cfg.ScratchDir, 0o700)
	return &Service{
		cfg:         cfg,
		journal:     ownershipJournal(filepath.Join(cfg.ScratchDir, "ownership")),
		pendingCkpt: make(map[string]*protocol.PrepareCheckpointRequest),
		activeSlots: make(map[string]*activeSlotState),
		byContainer: make(map[string]*activeSlotState),
		results:     make(map[string]*checkpointReceipt),
	}
}

func (s *Service) ActiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activeSlots)
}

func (s *Service) PrepareCheckpoint(ctx context.Context, req *protocol.PrepareCheckpointRequest) (_ *protocol.PrepareCheckpointResponse, retErr error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinued", "hydrator.prepare_checkpoint",
		attribute.String(telemetry.AttrSandboxID, req.SandboxID),
		attribute.String(telemetry.AttrSnapshotID, req.TargetSnapshotID),
	)
	defer func() { telemetry.EndSpan(span, retErr) }()

	s.mu.Lock()
	st := s.activeSlots[req.SlotID]
	s.mu.Unlock()
	if st == nil {
		return nil, fmt.Errorf("unknown slot %s", req.SlotID)
	}
	st.releaseMu.Lock()
	defer st.releaseMu.Unlock()
	s.mu.Lock()
	current := s.byContainer[st.ContainerID] == st
	s.mu.Unlock()
	if !current || st.Phase != activeExecution || st.SandboxID != req.SandboxID || (req.ExecutionID != 0 && req.ExecutionID != st.ExecutionID) || (req.LifecycleID != "" && req.LifecycleID != st.LifecycleID) {
		return nil, store.ErrStaleExecutionFence
	}
	if st.Slot.NetworkReadyOnly {
		if err := s.bindCheckpointPlaceholder(ctx, st); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	s.pendingCkpt[st.ContainerID] = req
	s.mu.Unlock()
	return &protocol.PrepareCheckpointResponse{Accepted: true}, nil
}

// The standalone guest can serve on a confirmed Pod network before kubelet
// starts its placeholder. A native checkpoint needs the real CRI container ID.
func (s *Service) bindCheckpointPlaceholder(ctx context.Context, st *activeSlotState) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	slot := st.Slot
	if err := s.confirmCarrier(ctx, slot); err != nil {
		return err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		list, err := s.cfg.Carriers.ListContainers(ctx, &runtimeapi.ListContainersRequest{
			Filter: &runtimeapi.ContainerFilter{
				PodSandboxId: slot.PodSandboxID,
				State:        &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING},
			},
		})
		if err != nil {
			return err
		}
		for _, container := range list.GetContainers() {
			if container.GetMetadata().GetName() != protocol.ContainerNameSandbox ||
				container.PodSandboxId != slot.PodSandboxID || container.Labels[protocol.K8sLabelPodUID] != slot.PodUID {
				continue
			}
			resp, err := s.cfg.Carriers.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: container.Id})
			if err != nil {
				return err
			}
			criStatus := resp.GetStatus()
			if criStatus == nil || criStatus.State != runtimeapi.ContainerState_CONTAINER_RUNNING {
				continue
			}
			slot.PlaceholderID = container.Id
			slot.ExpectedContainerHash = container.Labels[protocol.K8sLabelContainerHash]
			slot.LogPath = criStatus.LogPath
			slot.NetworkReadyOnly = false
			next := st.ownership
			next.Slot = slot
			if err := s.mapSlot(ctx, &activeSlotState{ownership: next}); err != nil {
				return err
			}
			if err := s.journal.write(next, false); err != nil {
				return err
			}
			// Readers of the slot identity either hold releaseMu or s.mu.
			s.mu.Lock()
			st.Slot = slot
			s.mu.Unlock()
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for checkpoint placeholder: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// ExecuteCheckpoint is invoked by kontinue-cri-shim when Kubelet's native POST /checkpoint/{ns}/{pod}/{ctr} arrives.
func (s *Service) ExecuteCheckpoint(ctx context.Context, req *protocol.ExecuteCheckpointRequest) (*protocol.ExecuteCheckpointResponse, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinued", "hydrator.execute_checkpoint",
		attribute.String("cri.container_id", req.ContainerID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	s.mu.Lock()
	active := s.byContainer[req.ContainerID]
	s.mu.Unlock()
	if active == nil {
		return nil, fmt.Errorf("unowned checkpoint container %s", req.ContainerID)
	}
	active.releaseMu.Lock()
	defer active.releaseMu.Unlock()
	s.mu.Lock()
	prep := s.pendingCkpt[req.ContainerID]
	current := s.byContainer[req.ContainerID] == active && active.Phase == activeExecution
	s.mu.Unlock()
	if !current {
		return nil, store.ErrStaleExecutionFence
	}

	if prep == nil {
		retErr = fmt.Errorf("no prepared checkpoint metadata found for container %s (%s/%s)", req.ContainerID, req.PodNamespace, req.PodName)
		return nil, retErr
	}
	span.SetAttributes(
		attribute.String(telemetry.AttrSandboxID, prep.SandboxID),
		attribute.String(telemetry.AttrSnapshotID, prep.TargetSnapshotID),
	)

	requestHash := checkpointRequestHash(prep)
	if saved := active.LastCheckpoint; saved != nil && saved.Result.Manifest.SnapshotID == prep.TargetSnapshotID {
		if saved.RequestHash != requestHash {
			return nil, store.ErrStaleExecutionFence
		}
		if req.KubeletLocation != "" {
			if err := s.cfg.Arena.WriteCheckpointDescriptor(req.KubeletLocation, &saved.Result.Manifest, saved.Result.ManifestURI); err != nil {
				return nil, err
			}
		}
		return &saved.Result, nil
	}
	ktdAddr := prep.PodIP
	if ktdAddr != "" && !strings.Contains(ktdAddr, ":") {
		ktdAddr = fmt.Sprintf("%s:%d", ktdAddr, protocol.DefaultKtdPort)
	}
	bootstrapTok := prep.BootstrapToken
	if active != nil {
		if active.KtdEndpoint != "" {
			ktdAddr = active.KtdEndpoint
		}
		if bootstrapTok == "" {
			bootstrapTok = active.BootstrapToken
		}
	}

	// Step 1: Quiesce guest filesystem, drain external sockets, and freeze workload cgroup via ktd!
	if ktdAddr != "" && bootstrapTok != "" {
		if _, err := s.cfg.KtdClient.Freeze(ctx, ktdAddr, bootstrapTok, protocol.FreezeRequest{
			Reason:              string(prep.Kind),
			DrainExternalTCP:    true,
			SyncFilesystems:     true,
			WaitInFlightTimeout: 3000,
		}); err != nil {
			retErr = fmt.Errorf("ktd pre-checkpoint freeze: %w", err)
			return nil, retErr
		}
	}

	// Step 2: Run gVisor split checkpoint (--compression=none) into node scratch dir!
	rawDir := filepath.Join(s.cfg.ScratchDir, fmt.Sprintf("raw-%s-%d", prep.TargetSnapshotID, time.Now().UnixNano()))
	defer func() { _ = os.RemoveAll(rawDir) }()

	captureCtx, captureSpan := telemetry.StartSpan(ctx, "kontinued", "hydrator.driver_checkpoint", attribute.String(telemetry.AttrSnapshotID, prep.TargetSnapshotID))
	captureErr := s.cfg.Driver.CheckpointSandboxContainer(captureCtx, req.ContainerID, rawDir, prep.LeaveRunning)
	telemetry.EndSpan(captureSpan, captureErr)
	if captureErr != nil {
		if ktdAddr != "" && bootstrapTok != "" {
			_ = s.cfg.KtdClient.Thaw(ctx, ktdAddr, bootstrapTok)
		}
		retErr = fmt.Errorf("driver checkpoint container: %w", captureErr)
		return nil, retErr
	}

	// If LeaveRunning (e.g. LiveFork or non-destructive template capture), thaw ktd immediately!
	if prep.LeaveRunning && prep.Kind != protocol.SnapshotKindSessionSuspend && ktdAddr != "" && bootstrapTok != "" {
		_ = s.cfg.KtdClient.Thaw(ctx, ktdAddr, bootstrapTok)
	}

	// Step 3: 2MiB Blake3 chunking + incremental diff against ParentSnapshotID + Kubelet tar descriptor!
	mf, uri, err := s.cfg.Arena.IngestRawCheckpoint(ctx, arena.IngestOptions{
		SnapshotID:       prep.TargetSnapshotID,
		Kind:             prep.Kind,
		TemplateID:       prep.TemplateID,
		BaseImage:        prep.BaseImage,
		ParentSnapshotID: prep.ParentSnapshotID,
		BootstrapToken:   bootstrapTok,
		RawCheckpointDir: rawDir,
		KubeletTarPath:   req.KubeletLocation,
		PinInL1Tmpfs:     prep.Kind == protocol.SnapshotKindGoldenTemplate,
		TraceHotPages:    prep.TraceHotPages,
		Fingerprint:      s.cfg.Fingerprint,
	})
	if err != nil {
		retErr = err
		return nil, err
	}

	out := &protocol.ExecuteCheckpointResponse{
		Manifest:    *mf,
		ManifestURI: uri,
		ArenaTier:   arena.TierL2NVMe,
	}
	next := active.ownership
	next.LastCheckpoint = &checkpointReceipt{RequestHash: requestHash, Result: *out}
	if err := s.journal.write(next, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if previous := active.LastCheckpoint; previous != nil {
		delete(s.results, previous.Result.Manifest.SnapshotID)
	}
	active.LastCheckpoint = next.LastCheckpoint
	s.results[prep.TargetSnapshotID] = next.LastCheckpoint
	s.mu.Unlock()
	return out, nil
}

// HydrateSlot hot-swaps a pre-warmed Carrier Pod's inner container with a memory-restored (or cold)
// sandbox instance, runs HeldInit identity scrubbing & secret injection, and publishes the fenced route.
func (s *Service) HydrateSlot(ctx context.Context, req *protocol.HydrateSlotRequest) (*protocol.HydrateSlotResponse, error) {
	start := time.Now()
	ctx, span := telemetry.StartSpan(ctx, "kontinued", "hydrator.hydrate_slot",
		attribute.String(telemetry.AttrSandboxID, req.SandboxID),
		attribute.String(telemetry.AttrSnapshotID, req.SnapshotID),
		attribute.Int64(telemetry.AttrExecutionID, req.ExecutionID),
		attribute.String(telemetry.AttrSlotID, req.Slot.SlotID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	var copiedBytes int64
	var viewDir string
	arenaTier := "COLD_BOOT"
	restoreMode := req.RestoreMode
	if restoreMode == "" {
		if req.SnapshotID != "" {
			restoreMode = protocol.RestoreModeMemory
		} else {
			restoreMode = protocol.RestoreModeCold
		}
	}
	bootstrapToken := req.BootstrapToken
	baseImage := req.BaseImage

	if req.Slot.SlotID == "" || req.SandboxID == "" || req.ExecutionID <= 0 {
		return nil, fmt.Errorf("hydrate requires slot, sandbox and execution IDs")
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	requestHash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if req.Slot.NetworkReadyOnly {
		// A replay can already have a terminal result, including loss of this Carrier.
		s.mu.Lock()
		previous := s.activeSlots[req.Slot.SlotID]
		s.mu.Unlock()
		if previous != nil {
			return s.replayHydration(ctx, previous, requestHash)
		}
		if _, ok := s.cfg.Driver.(*GVisorSandboxDriver); !ok || s.cfg.Carriers == nil {
			return nil, status.Error(codes.FailedPrecondition, "network-only Carrier requires the standalone driver and CRI")
		}
		if err := s.confirmCarrier(ctx, req.Slot); err != nil {
			return nil, fmt.Errorf("confirm cold Carrier network: %w", err)
		}
	}
	st := &activeSlotState{ownership: ownership{RequestHash: requestHash,
		Version: 1, Phase: preparing, Slot: req.Slot, SandboxID: req.SandboxID,
		ExecutionID: req.ExecutionID, LifecycleID: req.LifecycleID, ContainerID: "kt-" + rand.Text(),
	}}
	if req.SnapshotID != "" && restoreMode == protocol.RestoreModeMemory {
		st.ViewID = "view-" + rand.Text()
	}
	st.releaseMu.Lock()
	defer st.releaseMu.Unlock()
	s.mu.Lock()
	if previous := s.activeSlots[req.Slot.SlotID]; previous != nil {
		s.mu.Unlock()
		return s.replayHydration(ctx, previous, requestHash)
	}
	receipt, err := s.journal.previousRelease(&protocol.ReleaseSlotRequest{SlotID: req.Slot.SlotID, SandboxID: req.SandboxID, ExecutionID: req.ExecutionID, LifecycleID: req.LifecycleID})
	if err != nil || receipt.Released {
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, status.Error(codes.FailedPrecondition, "execution was already released")
	}
	if err := s.journal.write(st.ownership, true); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("claim slot %s: %w", req.Slot.SlotID, err)
	}
	s.activeSlots[req.Slot.SlotID] = st
	s.mu.Unlock()
	if req.SnapshotID != "" && restoreMode == protocol.RestoreModeMemory {
		viewID := st.ViewID
		var mf *protocol.SnapshotManifest
		var err error
		viewDir, arenaTier, mf, copiedBytes, err = s.cfg.Arena.PrepareRestoreView(ctx, req.SnapshotID, viewID)
		if err != nil {
			retErr = fmt.Errorf("prepare restore view: %w", err)
			return nil, retErr
		}
		st.ViewPath = viewDir
		st.View, err = s.cfg.Arena.DescribeView(viewDir)
		if err != nil {
			return nil, err
		}
		if bootstrapToken == "" && mf.BootstrapToken != "" {
			bootstrapToken = mf.BootstrapToken
		}
		if baseImage == "" && mf.BaseImage != "" {
			baseImage = mf.BaseImage
		}
	}
	if bootstrapToken == "" {
		bootstrapToken = "kontinue-default-bootstrap-token"
	}

	st.BootstrapToken = bootstrapToken
	if err := s.journal.write(st.ownership, false); err != nil {
		return nil, err
	}

	newCID, ktdEndpoint, err := s.cfg.Driver.SwapAndStartSandboxContainer(ctx, SwapContainerOptions{
		ContainerID:    st.ContainerID,
		ExecutionID:    req.ExecutionID,
		LifecycleID:    req.LifecycleID,
		Slot:           req.Slot,
		SandboxID:      req.SandboxID,
		BaseImage:      baseImage,
		RestoreViewDir: viewDir,
		RestoreMode:    restoreMode,
		BootstrapToken: bootstrapToken,
		AccessToken:    req.AccessToken,
		CPUMillis:      req.CPUMillis,
		MemoryBytes:    req.MemoryBytes,
	})
	if err != nil {
		retErr = fmt.Errorf("swap carrier container: %w", err)
		return nil, retErr
	}

	st.ContainerID = newCID
	if ktdEndpoint == "" && req.Slot.PodIP != "" {
		ktdEndpoint = net.JoinHostPort(req.Slot.PodIP, fmt.Sprint(protocol.DefaultKtdPort))
	}
	st.KtdEndpoint = ktdEndpoint
	// Persist the reply fields before HeldInit can complete. The preparing phase
	// keeps this reply provisional until the guest confirms its new identity.
	st.Activation = &protocol.HydrateSlotResponse{RestoreViewCopiedBytes: &copiedBytes,
		SandboxID: req.SandboxID, ExecutionID: req.ExecutionID, PodIP: ktdEndpoint,
		ContainerID: newCID, ArenaTier: arenaTier, RestoreMode: restoreMode,
	}
	if err := s.journal.write(st.ownership, false); err != nil {
		return nil, err
	}
	// Notify kontinue-cri-shim so Kubelet PLEG sees expected io.kubernetes.container.hash!
	if err := s.mapSlot(ctx, st); err != nil {
		return nil, err
	}

	// Perform E2B-style Expect: 100-continue HeldInit to scrub identity, rotate token, inject tenant secrets, and thaw!
	entropy := make([]byte, 32)
	_, _ = rand.Read(entropy)
	if err := s.waitAndHeldInit(ctx, ktdEndpoint, bootstrapToken, protocol.InitPayload{
		SandboxID:      req.SandboxID,
		ExecutionID:    req.ExecutionID,
		LifecycleID:    req.LifecycleID,
		Hostname:       req.SandboxID,
		AccessToken:    req.AccessToken,
		EnvVars:        req.EnvVars,
		EntropySeed:    entropy,
		WallClockUnixN: time.Now().UnixNano(),
		SkipThaw:       req.SkipThaw,
	}); err != nil {
		retErr = fmt.Errorf("post-restore HeldInit failed: %w", err)
		return nil, retErr
	}

	result := *st.Activation
	result.DurationMs = time.Since(start).Milliseconds()
	if err := s.commitActivation(st, &result); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.byContainer[newCID] = st
	s.mu.Unlock()

	if s.cfg.Routes != nil && !req.SkipThaw {
		r := st.route(s.cfg)
		r.OwnerKey = store.OwnerKey(r)
		if err := s.cfg.Routes.ReportExecution(ctx, r, store.ExecutionReport{State: store.OwnerReady}); err != nil {
			return nil, err
		}
		if !req.DeferRoutePublication {
			if err := s.cfg.Routes.PublishRoute(ctx, r); err != nil {
				retErr = fmt.Errorf("publish fenced route: %w", err)
				return nil, retErr
			}
		}
	}

	span.SetAttributes(attribute.String(telemetry.AttrArenaTier, arenaTier))
	return st.Activation, nil
}

func (s *Service) commitActivation(st *activeSlotState, result *protocol.HydrateSlotResponse) error {
	next := st.ownership
	next.Phase, next.Activation = activeExecution, result
	if err := s.journal.write(next, false); err != nil {
		return err
	}
	st.Phase, st.Activation = next.Phase, next.Activation
	return nil
}

// An ambiguous RPC reply must carry the original result on retry. A replay
// never thaws the guest or reopens a route that suspend may have drained.
func (s *Service) replayHydration(ctx context.Context, st *activeSlotState, requestHash string) (*protocol.HydrateSlotResponse, error) {
	st.releaseMu.Lock()
	defer st.releaseMu.Unlock()
	if st.RequestHash != requestHash || st.Phase == releasing {
		return nil, status.Error(codes.FailedPrecondition, "slot ownership conflicts with activation")
	}
	if st.Phase == lost {
		return nil, status.Errorf(codes.FailedPrecondition, "activation failed: %s", st.FailureReason)
	}
	s.mu.Lock()
	verified := s.activeSlots[st.Slot.SlotID] == st && s.byContainer[st.ContainerID] == st
	s.mu.Unlock()
	if !verified || st.Phase != activeExecution || st.Activation == nil {
		return nil, status.Error(codes.Unavailable, "activation awaits reconciliation")
	}
	if st.Slot.NetworkReadyOnly {
		if err := s.confirmCarrier(ctx, st.Slot); err != nil {
			return nil, fmt.Errorf("confirm cold Carrier network: %w", err)
		}
	}
	if err := s.report(ctx, st, store.OwnerReady); err != nil {
		return nil, err
	}
	return st.Activation, nil
}

func (s *Service) ReleaseSlot(ctx context.Context, req *protocol.ReleaseSlotRequest) (*protocol.ReleaseSlotResponse, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinued", "hydrator.release_slot",
		attribute.String(telemetry.AttrSandboxID, req.SandboxID),
		attribute.String(telemetry.AttrSlotID, req.SlotID),
	)
	defer telemetry.EndSpan(span, nil)

	if req.SlotID == "" || req.SandboxID == "" || req.ExecutionID <= 0 {
		return nil, fmt.Errorf("release requires slot, sandbox and execution IDs")
	}
	s.mu.Lock()
	st := s.activeSlots[req.SlotID]
	if st == nil || st.SandboxID != req.SandboxID || st.ExecutionID != req.ExecutionID || (req.LifecycleID != "" && req.LifecycleID != st.LifecycleID) {
		s.mu.Unlock()
		return s.journal.previousRelease(req)
	}
	s.mu.Unlock()

	st.releaseMu.Lock()
	defer st.releaseMu.Unlock()
	s.mu.Lock()
	current := s.activeSlots[req.SlotID] == st
	s.mu.Unlock()
	if !current {
		return s.journal.previousRelease(req)
	}
	// A crash can leave both the receipt and the claim. Only an exact match
	// can retire the claim without running teardown again.
	if st.Phase == releasing && req.LifecycleID == st.LifecycleID {
		receipt, err := s.journal.previousRelease(req)
		if err != nil {
			return nil, err
		}
		if receipt.Released {
			if receipt.PodUID != st.Slot.PodUID {
				return nil, fmt.Errorf("release receipt Pod UID does not match slot %s", st.Slot.SlotID)
			}
			return s.finishRelease(st, receipt)
		}
	}
	if s.cfg.Routes != nil {
		if err := s.cfg.Routes.ReportExecution(ctx, st.route(s.cfg), store.ExecutionReport{State: store.OwnerChecking}); err != nil {
			return nil, err
		}
	}
	if st.restored {
		if st.FailureReason == errWrongOwner.Error() {
			return nil, errWrongOwner
		}
		inspector, ok := s.cfg.Driver.(runtimeInspector)
		if !ok {
			return nil, fmt.Errorf("cannot confirm runtime ownership")
		}
		if err := inspector.inspectInstance(ctx, st.ownership); err != nil && !errors.Is(err, errInstanceLost) {
			return nil, err
		}

	}
	st.Phase = releasing
	if err := s.journal.write(st.ownership, false); err != nil {
		return nil, err
	}
	if err := s.cfg.Driver.StopSandboxContainer(ctx, st.Slot, st.ContainerID); err != nil {
		return nil, err
	}
	if st.ViewID != "" && (st.restored || st.ViewPath == "") {
		if err := s.cfg.Arena.ConfirmReleaseView(st.ViewID, st.ViewPath, st.View); err != nil {
			return nil, err
		}
	} else if st.ViewPath != "" {
		if err := s.cfg.Arena.ReleaseView(st.ViewPath); err != nil {
			return nil, err
		}
	}
	response := &protocol.ReleaseSlotResponse{Released: true, Tainted: !req.ReuseSlot, PodUID: st.Slot.PodUID}
	if err := s.journal.persist(s.journal.receipt(req), response, false); err != nil {
		return nil, err
	}
	return s.finishRelease(st, response)
}

// finishRelease runs under st.releaseMu after the release receipt is persisted.
func (s *Service) finishRelease(st *activeSlotState, receipt *protocol.ReleaseSlotResponse) (*protocol.ReleaseSlotResponse, error) {
	if err := s.journal.remove(st.Slot.SlotID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if st.LastCheckpoint != nil {
		delete(s.results, st.LastCheckpoint.Result.Manifest.SnapshotID)
	}
	if s.activeSlots[st.Slot.SlotID] == st {
		delete(s.activeSlots, st.Slot.SlotID)
	}
	if s.byContainer[st.ContainerID] == st {
		delete(s.byContainer, st.ContainerID)
		delete(s.pendingCkpt, st.ContainerID)
	}
	s.mu.Unlock()
	return receipt, nil
}

func (s *Service) PrewarmTemplate(ctx context.Context, req *protocol.PrewarmTemplateRequest) (*protocol.PrewarmTemplateResponse, error) {
	mf, err := s.cfg.Arena.ResolveManifest(ctx, req.SnapshotID)
	if err != nil {
		return nil, err
	}
	l1Path, err := s.cfg.Arena.PrewarmToL1Tmpfs(ctx, mf)
	if err != nil {
		return nil, err
	}
	return &protocol.PrewarmTemplateResponse{
		Ready:          true,
		LocalArenaPath: l1Path,
		BytesPinned:    mf.PagesImgTotalSize + mf.CheckpointImgSize,
	}, nil
}

func checkpointRequestHash(req *protocol.PrepareCheckpointRequest) string {
	raw, _ := json.Marshal(req)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func (s *Service) GetLatestSnapshotResult(_ context.Context, req *protocol.GetLatestSnapshotResultRequest) (*protocol.GetLatestSnapshotResultResponse, error) {
	s.mu.Lock()
	res, ok := s.results[req.SnapshotID]
	s.mu.Unlock()
	if !ok {
		return &protocol.GetLatestSnapshotResultResponse{Found: false}, nil
	}
	if req.Expected != nil && checkpointRequestHash(req.Expected) != res.RequestHash {
		return nil, store.ErrStaleExecutionFence
	}
	return &protocol.GetLatestSnapshotResultResponse{
		Found:       true,
		Manifest:    res.Result.Manifest,
		ManifestURI: res.Result.ManifestURI,
	}, nil
}

func (s *Service) SetCarrierWatcher(w *CarrierPodWatcher) {
	w.cfg.IsClaimed = func(slotID string) bool { s.mu.Lock(); defer s.mu.Unlock(); return s.activeSlots[slotID] != nil }
	_, standalone := s.cfg.Driver.(*GVisorSandboxDriver)
	w.cfg.ColdNetworkOnly = standalone && s.cfg.Carriers != nil
	s.cfg.CarrierWatcher = w
}

func (s *Service) ResyncIdleCarriers(ctx context.Context, _ *protocol.ResyncIdleCarriersRequest) (*protocol.ResyncIdleCarriersResponse, error) {
	if s.cfg.CarrierWatcher != nil {
		s.cfg.CarrierWatcher.ForceSync(ctx)
	}
	return &protocol.ResyncIdleCarriersResponse{Synced: true}, nil
}

func (s *Service) waitAndHeldInit(ctx context.Context, ktdEndpoint, bootstrapToken string, payload protocol.InitPayload) error {
	deadline := time.Now().Add(8 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := s.cfg.KtdClient.HeldInit(ctx, ktdEndpoint, bootstrapToken, payload)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(15 * time.Millisecond)
	}
	return lastErr
}

func (s *Service) mapSlot(ctx context.Context, st *activeSlotState) error {
	if s.cfg.ShimAdminURL == "" || st.Slot.NetworkReadyOnly {
		return nil
	}
	slot := st.Slot
	raw, _ := json.Marshal(crishim.SlotMapping{PodSandboxID: slot.PodSandboxID, PodNamespace: slot.PodNamespace, PodName: slot.PodName, PlaceholderID: slot.PlaceholderID, ActiveContainerID: st.ContainerID, ExpectedContainerHash: slot.ExpectedContainerHash, LogPath: slot.LogPath})
	var client *http.Client
	urlStr := s.cfg.ShimAdminURL + "/v1/shim/map-slot"
	if filepath.IsAbs(s.cfg.ShimAdminURL) {
		sock := s.cfg.ShimAdminURL
		client = &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sock)
				},
			},
		}
		urlStr = "http://unix/v1/shim/map-slot"
	} else {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("map carrier with CRI shim: HTTP %d", resp.StatusCode)
	}
	return nil
}

func ktdEndpointHost(endpoint, fallbackIP string) string {
	if endpoint == "" {
		return fallbackIP
	}
	if h, p, err := net.SplitHostPort(endpoint); err == nil && h != "" {
		if p == fmt.Sprintf("%d", protocol.DefaultKtdPort) {
			return h
		}
		return endpoint
	}
	return endpoint
}

// CRIRuntimeDriver implements RuntimeDriver against real containerd CRI + kontinue-runsc / gVisor.
type CRIRuntimeDriver struct {
	runtimeClient runtimeapi.RuntimeServiceClient
	imageClient   runtimeapi.ImageServiceClient
	runscBin      string
	runscRoot     string
}

func NewCRIRuntimeDriver(rt runtimeapi.RuntimeServiceClient, img runtimeapi.ImageServiceClient, runscBin, runscRoot string) *CRIRuntimeDriver {
	if runscBin == "" {
		runscBin = "/usr/local/bin/kontinue-runsc"
	}
	if runscRoot == "" {
		runscRoot = "/run/containerd/runsc/k8s.io"
	}
	return &CRIRuntimeDriver{
		runtimeClient: rt,
		imageClient:   img,
		runscBin:      runscBin,
		runscRoot:     runscRoot,
	}
}

func (d *CRIRuntimeDriver) SwapAndStartSandboxContainer(ctx context.Context, opts SwapContainerOptions) (string, string, error) {
	if opts.BaseImage != "" {
		_, _ = d.imageClient.PullImage(ctx, &runtimeapi.PullImageRequest{
			Image: &runtimeapi.ImageSpec{Image: opts.BaseImage},
		})
	}
	if opts.Slot.PlaceholderID != "" {
		_ = func() error {
			_, _ = d.runtimeClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: opts.Slot.PlaceholderID, Timeout: 1})
			_, _ = d.runtimeClient.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: opts.Slot.PlaceholderID})
			return nil
		}()
	}

	labels := map[string]string{
		protocol.K8sLabelPodName:       opts.Slot.PodName,
		protocol.K8sLabelPodNamespace:  opts.Slot.PodNamespace,
		protocol.K8sLabelPodUID:        opts.Slot.PodUID,
		protocol.K8sLabelContainerName: protocol.ContainerNameSandbox,
	}
	if opts.Slot.ExpectedContainerHash != "" {
		labels[protocol.K8sLabelContainerHash] = opts.Slot.ExpectedContainerHash
	}

	annotations := telemetry.InjectAnnotations(ctx, map[string]string{
		protocol.AnnotationSandboxID: opts.SandboxID,
	})
	if opts.RestoreViewDir != "" && opts.RestoreMode == protocol.RestoreModeMemory {
		annotations[protocol.GVisorAnnotationRestoreHostPath] = opts.RestoreViewDir
		annotations[protocol.GVisorAnnotationRestoreDirect] = "false"
	}

	createResp, err := d.runtimeClient.CreateContainer(ctx, &runtimeapi.CreateContainerRequest{
		PodSandboxId: opts.Slot.PodSandboxID,
		Config: &runtimeapi.ContainerConfig{
			Metadata:    &runtimeapi.ContainerMetadata{Name: protocol.ContainerNameSandbox, Attempt: 1},
			Image:       &runtimeapi.ImageSpec{Image: opts.BaseImage},
			Labels:      labels,
			Annotations: annotations,
			LogPath:     opts.Slot.LogPath,
			Envs: []*runtimeapi.KeyValue{
				{Key: "KTD_BOOTSTRAP_TOKEN", Value: opts.BootstrapToken},
				{Key: "KTD_INITIAL_ACCESS_TOKEN", Value: opts.AccessToken},
				{Key: "SANDBOX_ID", Value: opts.SandboxID},
			},
		},
		SandboxConfig: &runtimeapi.PodSandboxConfig{
			Metadata: &runtimeapi.PodSandboxMetadata{
				Name:      opts.Slot.PodName,
				Namespace: opts.Slot.PodNamespace,
				Uid:       opts.Slot.PodUID,
			},
		},
	})
	if err != nil {
		return "", "", err
	}

	if _, err := d.runtimeClient.StartContainer(ctx, &runtimeapi.StartContainerRequest{
		ContainerId: createResp.ContainerId,
	}); err != nil {
		return "", "", err
	}
	return createResp.ContainerId, fmt.Sprintf("%s:%d", opts.Slot.PodIP, protocol.DefaultKtdPort), nil
}

func (d *CRIRuntimeDriver) CheckpointSandboxContainer(ctx context.Context, containerID, rawOutputDir string, leaveRunning bool) error {
	if err := os.MkdirAll(rawOutputDir, 0o700); err != nil {
		return err
	}
	args := []string{"--root=" + d.runscRoot, "checkpoint", "--compression=none", "--image-path=" + rawOutputDir}
	if leaveRunning {
		args = append(args, "--leave-running")
	}
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, d.runscBin, args...)
	cmd.Env = telemetry.InjectEnv(ctx, os.Environ())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("runsc checkpoint %s: %w (%s)", containerID, err, string(out))
	}
	return nil
}

func (d *CRIRuntimeDriver) StopSandboxContainer(ctx context.Context, _ protocol.CarrierSlot, activeContainerID string) error {
	if activeContainerID == "" {
		return nil
	}
	_, _ = d.runtimeClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: activeContainerID, Timeout: 2})
	_, _ = d.runtimeClient.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: activeContainerID})
	return nil
}
