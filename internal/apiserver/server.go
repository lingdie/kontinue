package apiserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerConfig struct {
	Meta        store.MetadataStore
	Catalog     store.RuntimeCatalog
	Placement   *PlacementEngine
	Replenisher *CarrierPoolReplenisher
	Kubelet     KubeletCheckpointer
	Recorder    *telemetry.FlightRecorder
	SandboxDom  string
	ReuseSlots  bool
	// CAS enables physical orphan chunk / artifact deletion during snapshot GC sweeps.
	CAS interface {
		GetManifest(ctx context.Context, snapshotID string) (*protocol.SnapshotManifest, error)
		DeleteBlob(ctx context.Context, key string) error
	}
}

// Server implements kontinue-apiserver's synchronous REST lifecycle API, idle evictor, and snapshot GC.
type Server struct {
	cfg        ServerConfig
	ktdClient  *protocol.KtdClient
	mux        *http.ServeMux
	nodeConnMu sync.Mutex
	nodeConns  map[string]*grpc.ClientConn
}

func NewServer(cfg ServerConfig) *Server {
	if cfg.SandboxDom == "" {
		cfg.SandboxDom = "sandbox.local"
	}
	if cfg.Placement == nil {
		var cold ColdSlotAllocator
		if cfg.Replenisher != nil {
			cold = cfg.Replenisher
		}
		cfg.Placement = NewPlacementEngine(cfg.Catalog, cold)
	}
	s := &Server{
		cfg:       cfg,
		ktdClient: protocol.NewKtdClient(30 * time.Second),
		mux:       http.NewServeMux(),
		nodeConns: make(map[string]*grpc.ClientConn),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return telemetry.HTTPMiddleware("kontinue-apiserver", s.mux)
}

func (s *Server) Close() {
	s.nodeConnMu.Lock()
	defer s.nodeConnMu.Unlock()
	for _, cc := range s.nodeConns {
		_ = cc.Close()
	}
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if s.cfg.Recorder != nil {
		s.mux.HandleFunc(protocol.PathDebugTraces, s.cfg.Recorder.Handler())
	}
	s.mux.HandleFunc("/v1/templates", s.handleTemplates)
	s.mux.HandleFunc("/v1/templates/", s.handleTemplateByID)
	s.mux.HandleFunc("/v1/sandboxes", s.handleSandboxes)
	s.mux.HandleFunc("/v1/sandboxes/", s.handleSandboxActions)
}

type CreateTemplateRequest struct {
	TemplateID    string `json:"templateId,omitempty"`
	Name          string `json:"name"`
	BaseImage     string `json:"baseImage"`
	WarmupScript  string `json:"warmupScript,omitempty"`
	Tier          string `json:"tier,omitempty"`
	PinInL1Tmpfs  bool   `json:"pinInL1Tmpfs"`
	TraceHotPages bool   `json:"traceHotPages"`
}

type CreateSandboxRequest struct {
	SandboxID                   string            `json:"sandboxId,omitempty"`
	TemplateID                  string            `json:"templateId,omitempty"`
	BaseImage                   string            `json:"baseImage,omitempty"`
	Tier                        string            `json:"tier,omitempty"`
	EnvVars                     map[string]string `json:"envVars,omitempty"`
	IdleTimeoutSeconds          int64             `json:"idleTimeoutSeconds,omitempty"`
	CPUMillis                   int64             `json:"cpuMillis,omitempty"`
	MemoryBytes                 int64             `json:"memoryBytes,omitempty"`
	AllowFilesystemOnlyFallback bool              `json:"allowFilesystemOnlyFallback,omitempty"`
}

type ForkSandboxRequest struct {
	Count   int               `json:"count"`
	EnvVars map[string]string `json:"envVars,omitempty"`
}

func (s *Server) handleTemplates(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		list, err := s.cfg.Meta.ListTemplates(r.Context())
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, list)
	case http.MethodPost:
		var req CreateTemplateRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		tpl, err := s.BuildGoldenTemplate(r.Context(), req)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, tpl)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleTemplateByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/templates/")
	tpl, err := s.cfg.Meta.GetTemplate(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "template not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tpl)
}

func (s *Server) handleSandboxes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	idemKey := r.Header.Get(protocol.HeaderIdempotencyKey)
	if idemKey != "" {
		if code, body, ok, _ := s.cfg.Catalog.GetIdempotency(r.Context(), idemKey); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write(body)
			return
		}
	}

	var req CreateSandboxRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sbx, err := s.CreateSandbox(r.Context(), req)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrSandboxExists) || errors.Is(err, store.ErrTransitionLocked) {
			status = http.StatusConflict
		}
		if errors.Is(err, store.ErrNoIdleCarrierSlot) {
			status = http.StatusServiceUnavailable
		}
		writeErr(w, status, err.Error())
		return
	}
	raw, _ := json.Marshal(redactSandbox(sbx))
	if idemKey != "" {
		_ = s.cfg.Catalog.SaveIdempotency(r.Context(), idemKey, http.StatusCreated, raw, 10*time.Minute)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(raw)
}

func (s *Server) handleSandboxActions(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusBadRequest, "missing sandbox id")
		return
	}
	sandboxID := parts[0]

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			sbx, err := s.cfg.Meta.GetSandbox(r.Context(), sandboxID)
			if errors.Is(err, store.ErrNotFound) {
				writeErr(w, http.StatusNotFound, "sandbox not found")
				return
			}
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, redactSandbox(sbx))
		case http.MethodDelete:
			if err := s.DeleteSandbox(r.Context(), sandboxID); err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	switch parts[1] {
	case "suspend":
		snap, err := s.SuspendSandbox(r.Context(), sandboxID)
		if err != nil {
			if errors.Is(err, store.ErrTransitionLocked) {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, snap)
	case "resume":
		sbx, err := s.ResumeSandbox(r.Context(), sandboxID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, redactSandbox(sbx))
	case "fork":
		var fReq ForkSandboxRequest
		if r.Body != nil && r.ContentLength != 0 {
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&fReq)
		}
		forks, err := s.ForkSandbox(r.Context(), sandboxID, fReq)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		pub := make([]*store.SandboxRecord, 0, len(forks))
		for _, f := range forks {
			cp := redactSandbox(f)
			pub = append(pub, &cp)
		}
		writeJSON(w, http.StatusCreated, pub)
	default:
		writeErr(w, http.StatusNotFound, "unknown sandbox action")
	}
}

// BuildGoldenTemplate boots a builder sandbox in a Carrier Slot, runs WarmupScript to prime RAM,
// triggers Kubelet's native POST :10250/checkpoint API, stores the Golden Snapshot, and prewarms L1 tmpfs.
func (s *Server) BuildGoldenTemplate(ctx context.Context, req CreateTemplateRequest) (*store.TemplateRecord, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.build_golden_template",
		attribute.String("template.name", req.Name),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	if req.TemplateID == "" {
		req.TemplateID = "tpl-" + randHex(6)
	}
	if req.Tier == "" {
		req.Tier = protocol.DefaultCarrierTier
	}
	tpl := &store.TemplateRecord{
		TemplateID:    req.TemplateID,
		Name:          req.Name,
		BaseImage:     req.BaseImage,
		WarmupScript:  req.WarmupScript,
		Tier:          req.Tier,
		PinInL1Tmpfs:  req.PinInL1Tmpfs,
		TraceHotPages: req.TraceHotPages,
		Phase:         "WarmingUp",
	}
	if err := s.cfg.Meta.CreateTemplate(ctx, tpl); err != nil {
		retErr = err
		return nil, err
	}

	dec, err := s.cfg.Placement.SelectAndClaimSlot(ctx, PlacementRequest{Tier: req.Tier})
	if err != nil {
		_ = s.cfg.Meta.UpdateTemplateStatus(ctx, tpl.TemplateID, "Failed", "", err.Error())
		retErr = err
		return nil, err
	}
	nodeClient, err := s.nodeClient(dec.Node.GRPCAddr)
	if err != nil {
		retErr = err
		return nil, err
	}

	builderID := "builder-" + tpl.TemplateID
	bootTok := "boot-" + randHex(12)
	warmupTok := "warmup-" + randHex(12)

	hydrateResp, err := nodeClient.HydrateSlot(ctx, &protocol.HydrateSlotRequest{
		Slot:           dec.Slot,
		SandboxID:      builderID,
		TemplateID:     tpl.TemplateID,
		ExecutionID:    1,
		LifecycleID:    "life-" + randHex(6),
		BaseImage:      tpl.BaseImage,
		RestoreMode:    protocol.RestoreModeCold,
		BootstrapToken: bootTok,
		AccessToken:    warmupTok,
		SkipThaw:       false,
	})
	if err != nil {
		_ = s.cfg.Meta.UpdateTemplateStatus(ctx, tpl.TemplateID, "Failed", "", err.Error())
		retErr = err
		return nil, err
	}
	defer func() {
		_, _ = nodeClient.ReleaseSlot(context.Background(), &protocol.ReleaseSlotRequest{
			SlotID:      dec.Slot.SlotID,
			SandboxID:   builderID,
			ExecutionID: 1,
			ReuseSlot:   s.cfg.ReuseSlots,
		})
		if !s.cfg.ReuseSlots && s.cfg.Replenisher != nil {
			s.cfg.Replenisher.EnqueueTaintedPod(dec.Slot.PodNamespace, dec.Slot.PodName)
		}
	}()

	// Execute WarmupScript inside the running container to bake libraries/models into RAM!
	if req.WarmupScript != "" {
		execOut, execErr := s.ktdClient.Exec(ctx, hydrateResp.PodIP, warmupTok, protocol.ExecRequest{
			Command:   []string{"/bin/sh", "-c", req.WarmupScript},
			TimeoutMs: 120_000,
		})
		if execErr != nil || execOut.ExitCode != 0 {
			msg := fmt.Sprintf("warmup script failed (err=%v, exit=%v)", execErr, execOut)
			_ = s.cfg.Meta.UpdateTemplateStatus(ctx, tpl.TemplateID, "Failed", "", msg)
			retErr = errors.New(msg)
			return nil, retErr
		}
	}

	releaseSnapshots, err := s.cfg.Meta.LockSnapshotSet(ctx, false)
	if err != nil {
		return nil, err
	}
	defer releaseSnapshots()
	goldenSnapID := "snap-golden-" + randHex(6)
	if _, err := nodeClient.PrepareCheckpoint(ctx, &protocol.PrepareCheckpointRequest{
		SandboxID:        builderID,
		TemplateID:       tpl.TemplateID,
		BaseImage:        tpl.BaseImage,
		SlotID:           dec.Slot.SlotID,
		PodNamespace:     dec.Slot.PodNamespace,
		PodName:          dec.Slot.PodName,
		PodIP:            dec.Slot.PodIP,
		TargetSnapshotID: goldenSnapID,
		Kind:             protocol.SnapshotKindGoldenTemplate,
		LeaveRunning:     false,
		TraceHotPages:    req.TraceHotPages,
		BootstrapToken:   bootTok,
	}); err != nil {
		retErr = err
		return nil, err
	}

	if _, err := s.cfg.Kubelet.CheckpointContainer(ctx, dec.Node.NodeIP, dec.Node.KubeletPort,
		dec.Slot.PodNamespace, dec.Slot.PodName, protocol.ContainerNameSandbox, 60); err != nil {
		_ = s.cfg.Meta.UpdateTemplateStatus(ctx, tpl.TemplateID, "Failed", "", err.Error())
		retErr = err
		return nil, err
	}

	snapRes, err := nodeClient.GetLatestSnapshotResult(ctx, &protocol.GetLatestSnapshotResultRequest{SnapshotID: goldenSnapID})
	if err != nil || !snapRes.Found {
		retErr = fmt.Errorf("retrieve golden snapshot result %s: %w", goldenSnapID, err)
		return nil, retErr
	}

	if err := s.cfg.Meta.CreateSnapshot(ctx, &store.SnapshotRecord{
		SnapshotID:       goldenSnapID,
		Kind:             protocol.SnapshotKindGoldenTemplate,
		TemplateID:       tpl.TemplateID,
		BaseImage:        tpl.BaseImage,
		ManifestURI:      snapRes.ManifestURI,
		OriginNodeID:     dec.Node.NodeID,
		OriginPeerAddr:   dec.Node.P2PAddr,
		TotalMemoryBytes: snapRes.Manifest.PagesImgTotalSize,
		DirtyMemoryBytes: snapRes.Manifest.DirtyMemoryBytes,
		DirtyChunks:      snapRes.Manifest.DirtyChunkCount,
		ReusedChunks:     snapRes.Manifest.ReusedChunkCount,
		BootstrapToken:   bootTok,
		Fingerprint:      snapRes.Manifest.Fingerprint,
	}); err != nil {
		retErr = err
		return nil, err
	}

	if req.PinInL1Tmpfs {
		s.broadcastL1Prewarm(ctx, goldenSnapID)
	}

	_ = s.cfg.Meta.UpdateTemplateStatus(ctx, tpl.TemplateID, "Ready", goldenSnapID, "golden memory snapshot ready")
	return s.cfg.Meta.GetTemplate(ctx, tpl.TemplateID)
}

// CreateSandbox clones a new 1-to-N sandbox from a Golden Template in ~100ms with 0 K8s etcd writes!
func (s *Server) CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*store.SandboxRecord, error) {
	if req.SandboxID == "" {
		req.SandboxID = "sbx-" + randHex(6)
	}
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.create_sandbox",
		attribute.String(telemetry.AttrSandboxID, req.SandboxID),
		attribute.String(telemetry.AttrTemplateID, req.TemplateID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	unlock, err := s.lockExecution(ctx, req.SandboxID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, err := s.cfg.Meta.GetSandbox(ctx, req.SandboxID); err == nil {
		return nil, store.ErrSandboxExists
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	var snap *store.SnapshotRecord
	baseImage := req.BaseImage
	tier := req.Tier
	if req.TemplateID != "" {
		tpl, err := s.cfg.Meta.GetTemplate(ctx, req.TemplateID)
		if err != nil {
			retErr = fmt.Errorf("load template %s: %w", req.TemplateID, err)
			return nil, retErr
		}
		baseImage = tpl.BaseImage
		if tier == "" {
			tier = tpl.Tier
		}
		if tpl.GoldenSnapshotID != "" {
			snap, err = s.cfg.Meta.GetSnapshot(ctx, tpl.GoldenSnapshotID)
			if err != nil {
				retErr = err
				return nil, err
			}
		}
	}
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}

	var reqFP *protocol.CompatibilityFingerprint
	var snapID, bootTok string
	if snap != nil {
		snapID = snap.SnapshotID
		bootTok = snap.BootstrapToken
		reqFP = &snap.Fingerprint
	}
	if bootTok == "" {
		bootTok = "boot-" + randHex(12)
	}
	accessTok := "kt_live_" + randHex(16)
	lifecycleID := "life-" + randHex(6)

	var dec *PlacementDecision
	var hydResp *protocol.HydrateSlotResponse
	for attempt := 0; attempt < 3; attempt++ {
		var err error
		dec, err = s.cfg.Placement.SelectAndClaimSlot(ctx, PlacementRequest{
			Tier:                        tier,
			SnapshotID:                  snapID,
			RequiredFingerprint:         reqFP,
			AllowFilesystemOnlyFallback: req.AllowFilesystemOnlyFallback,
		})
		if err != nil {
			retErr = err
			return nil, err
		}
		nodeClient, err := s.nodeClient(dec.Node.GRPCAddr)
		if err != nil {
			retErr = err
			return nil, err
		}
		hydResp, err = nodeClient.HydrateSlot(ctx, &protocol.HydrateSlotRequest{
			Slot:           dec.Slot,
			SandboxID:      req.SandboxID,
			TemplateID:     req.TemplateID,
			ExecutionID:    1,
			LifecycleID:    lifecycleID,
			SnapshotID:     snapID,
			BaseImage:      baseImage,
			RestoreMode:    dec.RestoreMode,
			BootstrapToken: bootTok,
			AccessToken:    accessTok,
			EnvVars:        req.EnvVars,
			CPUMillis:      req.CPUMillis,
			MemoryBytes:    req.MemoryBytes,
		})
		if err == nil {
			retErr = nil
			break
		}
		retErr = err
		s.abandonHydratedSlot(ctx, dec, req.SandboxID, 1)
	}
	if retErr != nil {
		return nil, retErr
	}

	poolAlloc := "WARM_POOL"
	if dec.ColdFallback {
		poolAlloc = "COLD_FALLBACK"
	}
	sbx := &store.SandboxRecord{
		SandboxID:                   req.SandboxID,
		TemplateID:                  req.TemplateID,
		BaseImage:                   baseImage,
		Tier:                        tier,
		State:                       store.SandboxStateRunning,
		ExecutionID:                 1,
		LifecycleID:                 lifecycleID,
		ActiveSnapshotID:            snapID,
		CurrentNodeID:               dec.Node.NodeID,
		OriginNodeID:                dec.Node.NodeID,
		CurrentSlotID:               dec.Slot.SlotID,
		CarrierPodNamespace:         dec.Slot.PodNamespace,
		CarrierPodName:              dec.Slot.PodName,
		PodIP:                       hydResp.PodIP,
		PoolAllocation:              poolAlloc,
		AccessToken:                 accessTok,
		BootstrapToken:              bootTok,
		EnvVars:                     req.EnvVars,
		IdleTimeoutSeconds:          req.IdleTimeoutSeconds,
		CPUMillis:                   req.CPUMillis,
		MemoryBytes:                 req.MemoryBytes,
		AllowFilesystemOnlyFallback: req.AllowFilesystemOnlyFallback,
	}
	if err := s.cfg.Meta.CreateSandbox(ctx, sbx); err != nil {
		s.abandonHydratedSlot(ctx, dec, req.SandboxID, 1)
		retErr = err
		return nil, err
	}
	_ = s.cfg.Catalog.TouchSandboxActive(ctx, sbx.SandboxID, time.Now().UTC())
	return sbx, nil
}

// A session-scoped lock covers checkpoint, upload and commit, regardless of their duration.
func (s *Server) lockExecution(ctx context.Context, id string) (func(), error) {
	release, ok, err := s.cfg.Meta.TryLeader(ctx, "sandbox/"+id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, store.ErrTransitionLocked
	}
	return release, nil
}

// SuspendSandbox checkpoints a running sandbox via Kubelet's native /checkpoint API,
// diffs 2MiB chunks against its parent template, releases 100% of container RAM, and marks it SUSPENDED.
func (s *Server) SuspendSandbox(ctx context.Context, sandboxID string) (*store.SnapshotRecord, error) {
	return s.suspendSandbox(ctx, sandboxID, nil)
}

func (s *Server) suspendSandbox(ctx context.Context, sandboxID string, observed *store.SandboxRecord) (_ *store.SnapshotRecord, retErr error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.suspend_sandbox",
		attribute.String(telemetry.AttrSandboxID, sandboxID),
	)

	defer func() { telemetry.EndSpan(span, retErr) }()

	unlock, err := s.lockExecution(ctx, sandboxID)
	if err != nil {
		retErr = err
		return nil, err
	}
	defer unlock()

	sbx, err := s.cfg.Meta.GetSandbox(ctx, sandboxID)
	if err != nil {
		retErr = err
		return nil, err
	}
	if observed != nil && (sbx.ExecutionID != observed.ExecutionID || sbx.State != observed.State || sbx.LifecycleID != observed.LifecycleID) {
		return nil, store.ErrStaleExecutionFence
	}
	if sbx.State == store.SandboxStateSuspended && sbx.ActiveSnapshotID != "" {
		if err := s.finishSuspendRelease(ctx, sbx); err != nil {
			return nil, err
		}
		return s.cfg.Meta.GetSnapshot(ctx, sbx.ActiveSnapshotID)
	}
	if sbx.State != store.SandboxStateRunning && sbx.State != store.SandboxStateSuspending {
		return nil, fmt.Errorf("cannot suspend sandbox in state %s", sbx.State)
	}
	route, err := s.routeForExecution(ctx, sbx)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Catalog.DrainRoute(ctx, *route); err != nil {
		return nil, err
	}

	if sbx.State == store.SandboxStateRunning {
		sbx.State = store.SandboxStateSuspending
		sbx.PendingSnapshotID = "snap-susp-" + randHex(12)
		if err := s.cfg.Meta.SaveSandbox(ctx, sbx); err != nil {
			return nil, err
		}
	}
	if sbx.PendingSnapshotID == "" {
		return nil, fmt.Errorf("suspending sandbox has no pending snapshot")
	}
	snapRec, err := s.cfg.Meta.GetSnapshot(ctx, sbx.PendingSnapshotID)
	if errors.Is(err, store.ErrNotFound) {
		snapRec, err = s.captureSuspend(ctx, sbx, route)
	}
	if err != nil {
		return nil, err
	}

	previous := sbx.ActiveSnapshotID
	sbx.State = store.SandboxStateSuspended
	sbx.ActiveSnapshotID = snapRec.SnapshotID
	sbx.PendingSnapshotID = ""
	sbx.OriginNodeID = route.NodeID
	// Preserve the old allocation until node cleanup is acknowledged.
	if err := s.cfg.Meta.SaveSandbox(ctx, sbx); err != nil {
		return nil, err
	}
	if previous != "" {
		if snap, err := s.cfg.Meta.GetSnapshot(ctx, previous); err == nil && snap.Kind == protocol.SnapshotKindSessionSuspend {
			_ = s.cfg.Meta.MarkSnapshotSuperseded(ctx, previous)
		}
	}
	if err := s.finishSuspendRelease(ctx, sbx); err != nil {
		return nil, err
	}
	return snapRec, nil
}

func (s *Server) finishSuspendRelease(ctx context.Context, sbx *store.SandboxRecord) error {
	if sbx.CurrentSlotID == "" {
		return nil
	}
	if err := s.releaseExecution(ctx, sbx); err != nil {
		return err
	}
	sbx.CurrentNodeID = ""
	sbx.CurrentSlotID = ""
	sbx.PodIP = ""
	return s.cfg.Meta.SaveSandbox(ctx, sbx)
}

func (s *Server) releaseExecution(ctx context.Context, sbx *store.SandboxRecord) error {
	route, err := s.routeForExecution(ctx, sbx)
	if err != nil {
		return err
	}
	nc, err := s.nodeClient(route.NodeGRPCAddr)
	if err != nil {
		return err
	}
	_, err = nc.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: sbx.CurrentSlotID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ReuseSlot: s.cfg.ReuseSlots})
	if err != nil {
		return err
	}
	if !s.cfg.ReuseSlots && s.cfg.Replenisher != nil {
		s.cfg.Replenisher.EnqueueTaintedPod(sbx.CarrierPodNamespace, sbx.CarrierPodName)
	}
	return nil
}

func (s *Server) captureSuspend(ctx context.Context, sbx *store.SandboxRecord, route *protocol.RouteRecord) (*store.SnapshotRecord, error) {
	releaseSnapshots, err := s.cfg.Meta.LockSnapshotSet(ctx, false)
	if err != nil {
		return nil, err
	}
	defer releaseSnapshots()
	nodeClient, err := s.nodeClient(route.NodeGRPCAddr)
	if err != nil {
		return nil, err
	}
	sandboxID := sbx.SandboxID
	newSnapID := sbx.PendingSnapshotID
	if _, err := nodeClient.PrepareCheckpoint(ctx, &protocol.PrepareCheckpointRequest{
		SandboxID:        sandboxID,
		TemplateID:       sbx.TemplateID,
		BaseImage:        sbx.BaseImage,
		SlotID:           sbx.CurrentSlotID,
		PodNamespace:     sbx.CarrierPodNamespace,
		PodName:          sbx.CarrierPodName,
		PodIP:            route.PodIP,
		TargetSnapshotID: newSnapID,
		ParentSnapshotID: sbx.ActiveSnapshotID,
		Kind:             protocol.SnapshotKindSessionSuspend,
		LeaveRunning:     true,
		BootstrapToken:   sbx.BootstrapToken,
	}); err != nil {
		return nil, err
	}

	kubeletPort := s.lookupNodeKubeletPort(ctx, route.NodeID)
	if _, err := s.cfg.Kubelet.CheckpointContainer(ctx, route.NodeIP, kubeletPort,
		sbx.CarrierPodNamespace, sbx.CarrierPodName, protocol.ContainerNameSandbox, 60); err != nil {
		return nil, err
	}

	snapRes, err := nodeClient.GetLatestSnapshotResult(ctx, &protocol.GetLatestSnapshotResultRequest{SnapshotID: newSnapID})
	if err != nil {
		return nil, fmt.Errorf("fetch suspend snapshot %s: %w", newSnapID, err)
	}
	if !snapRes.Found {
		return nil, fmt.Errorf("suspend snapshot %s is unavailable", newSnapID)
	}

	prevSnapID := sbx.ActiveSnapshotID
	snapRec := &store.SnapshotRecord{
		SnapshotID:       newSnapID,
		Kind:             protocol.SnapshotKindSessionSuspend,
		TemplateID:       sbx.TemplateID,
		SourceSandboxID:  sandboxID,
		ParentSnapshotID: prevSnapID,
		BaseImage:        sbx.BaseImage,
		ManifestURI:      snapRes.ManifestURI,
		OriginNodeID:     route.NodeID,
		OriginPeerAddr:   route.NodeGRPCAddr,
		TotalMemoryBytes: snapRes.Manifest.PagesImgTotalSize,
		DirtyMemoryBytes: snapRes.Manifest.DirtyMemoryBytes,
		DirtyChunks:      snapRes.Manifest.DirtyChunkCount,
		ReusedChunks:     snapRes.Manifest.ReusedChunkCount,
		BootstrapToken:   sbx.BootstrapToken,
		Fingerprint:      snapRes.Manifest.Fingerprint,
	}
	if err := s.cfg.Meta.CreateSnapshot(ctx, snapRec); err != nil {
		return nil, err
	}

	return snapRec, nil
}

// ResumeSandbox wakes a SUSPENDED sandbox on OriginNode (L2 NVMe hit) or cross-node (L3 ArenaFS + P2P)
// under a strictly incremented ExecutionID fence.
func (s *Server) ResumeSandbox(ctx context.Context, sandboxID string) (*store.SandboxRecord, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.resume_sandbox",
		attribute.String(telemetry.AttrSandboxID, sandboxID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	unlock, err := s.lockExecution(ctx, sandboxID)
	if err != nil {
		retErr = err
		return nil, err
	}
	defer unlock()

	sbx, err := s.cfg.Meta.GetSandbox(ctx, sandboxID)
	if err != nil {
		retErr = err
		return nil, err
	}
	if sbx.State == store.SandboxStateRunning {
		if _, err := s.cfg.Catalog.GetRoute(ctx, sandboxID); errors.Is(err, store.ErrNotFound) {
			r, err := s.routeForExecution(ctx, sbx)
			if err != nil {
				return nil, err
			}
			health, err := s.ktdClient.Health(ctx, sbx.PodIP, sbx.AccessToken)
			if err != nil {
				return nil, err
			}
			if health.SandboxID != sbx.SandboxID || health.ExecutionID != sbx.ExecutionID || !health.Healthy || health.Frozen {
				return nil, store.ErrStaleExecutionFence
			}
			if err := s.cfg.Catalog.PublishRoute(ctx, *r); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		return sbx, nil
	}

	if sbx.State != store.SandboxStateSuspended {
		return nil, store.ErrTransitionLocked
	}
	if err := s.finishSuspendRelease(ctx, sbx); err != nil {
		return nil, err
	}

	snap, err := s.cfg.Meta.GetSnapshot(ctx, sbx.ActiveSnapshotID)
	if err != nil {
		retErr = fmt.Errorf("load active snapshot %s: %w", sbx.ActiveSnapshotID, err)
		return nil, retErr
	}

	nextExecID := sbx.ExecutionID + 1
	nextLifeID := "life-" + randHex(6)
	var dec *PlacementDecision
	var hydResp *protocol.HydrateSlotResponse
	for attempt := 0; attempt < 3; attempt++ {
		dec, err = s.cfg.Placement.SelectAndClaimSlot(ctx, PlacementRequest{
			Tier:                        sbx.Tier,
			SnapshotID:                  snap.SnapshotID,
			OriginNodeID:                sbx.OriginNodeID,
			RequiredFingerprint:         &snap.Fingerprint,
			AllowFilesystemOnlyFallback: sbx.AllowFilesystemOnlyFallback,
		})
		if err != nil {
			retErr = err
			return nil, err
		}
		nodeClient, cErr := s.nodeClient(dec.Node.GRPCAddr)
		if cErr != nil {
			retErr = cErr
			return nil, cErr
		}
		hydResp, err = nodeClient.HydrateSlot(ctx, &protocol.HydrateSlotRequest{
			Slot:           dec.Slot,
			SandboxID:      sbx.SandboxID,
			TemplateID:     sbx.TemplateID,
			ExecutionID:    nextExecID,
			LifecycleID:    nextLifeID,
			SnapshotID:     snap.SnapshotID,
			BaseImage:      sbx.BaseImage,
			RestoreMode:    dec.RestoreMode,
			BootstrapToken: sbx.BootstrapToken,
			AccessToken:    sbx.AccessToken,
			EnvVars:        sbx.EnvVars,
			CPUMillis:      sbx.CPUMillis,
			MemoryBytes:    sbx.MemoryBytes,
		})
		if err == nil {
			retErr = nil
			break
		}
		retErr = err
		s.abandonHydratedSlot(ctx, dec, sbx.SandboxID, nextExecID)
	}
	if retErr != nil {
		return nil, retErr
	}

	if dec.ColdFallback {
		sbx.PoolAllocation = "COLD_FALLBACK"
	} else {
		sbx.PoolAllocation = "WARM_POOL"
	}
	sbx.State = store.SandboxStateRunning
	sbx.ExecutionID = nextExecID
	sbx.LifecycleID = nextLifeID
	sbx.CurrentNodeID = dec.Node.NodeID
	sbx.OriginNodeID = dec.Node.NodeID
	sbx.CurrentSlotID = dec.Slot.SlotID
	sbx.CarrierPodNamespace = dec.Slot.PodNamespace
	sbx.CarrierPodName = dec.Slot.PodName
	sbx.PodIP = hydResp.PodIP
	if err := s.cfg.Meta.SaveSandbox(ctx, sbx); err != nil {
		s.abandonHydratedSlot(ctx, dec, sbx.SandboxID, nextExecID)
		retErr = err
		return nil, err
	}
	_ = s.cfg.Catalog.TouchSandboxActive(ctx, sbx.SandboxID, time.Now().UTC())
	return sbx, nil
}

func (s *Server) abandonHydratedSlot(ctx context.Context, dec *PlacementDecision, sandboxID string, executionID int64) {
	if dec == nil {
		return
	}
	if nc, err := s.nodeClient(dec.Node.GRPCAddr); err == nil {
		_, _ = nc.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{
			SlotID:      dec.Slot.SlotID,
			SandboxID:   sandboxID,
			ExecutionID: executionID,
			ReuseSlot:   false,
		})
	}
	if s.cfg.Replenisher != nil {
		s.cfg.Replenisher.EnqueueTaintedPod(dec.Slot.PodNamespace, dec.Slot.PodName)
	}
}

func (s *Server) routeForExecution(ctx context.Context, sbx *store.SandboxRecord) (*protocol.RouteRecord, error) {
	if existing, err := s.cfg.Catalog.GetRoute(ctx, sbx.SandboxID); err == nil {
		if existing.ExecutionID != sbx.ExecutionID || existing.SlotID != sbx.CurrentSlotID {
			return nil, store.ErrStaleExecutionFence
		}
		return existing, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if sbx.PodIP == "" || sbx.CurrentNodeID == "" {
		return nil, store.ErrNotFound
	}
	nodes, err := s.cfg.Catalog.ListHealthyNodes(ctx)
	if err != nil {
		return nil, err
	}
	var nodeIP, grpcAddr string
	for _, n := range nodes {
		if n.NodeID == sbx.CurrentNodeID {
			nodeIP = n.NodeIP
			grpcAddr = n.GRPCAddr
			break
		}
	}
	if grpcAddr == "" {
		return nil, store.ErrNotFound
	}
	route := &protocol.RouteRecord{
		SandboxID:      sbx.SandboxID,
		ExecutionID:    sbx.ExecutionID,
		LifecycleID:    sbx.LifecycleID,
		NodeID:         sbx.CurrentNodeID,
		NodeIP:         nodeIP,
		NodeGRPCAddr:   grpcAddr,
		SlotID:         sbx.CurrentSlotID,
		CarrierPodName: sbx.CarrierPodName,
		PodNamespace:   sbx.CarrierPodNamespace,
		PodIP:          sbx.PodIP,
		StartedAt:      time.Now().UTC(),
	}
	return route, nil
}

// ForkSandbox checkpoints a live running sandbox in-place (--leave-running) via Kubelet /checkpoint API
// and spawns K independent branch sandboxes with distinct IDs, IPs, and tokens from that exact memory state.
func (s *Server) ForkSandbox(ctx context.Context, sourceSandboxID string, req ForkSandboxRequest) ([]*store.SandboxRecord, error) {
	if req.Count <= 0 {
		req.Count = 1
	}
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.fork_sandbox",
		attribute.String(telemetry.AttrSandboxID, sourceSandboxID),
		attribute.Int("fork.count", req.Count),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	unlock, err := s.lockExecution(ctx, sourceSandboxID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	releaseSnapshots, err := s.cfg.Meta.LockSnapshotSet(ctx, false)
	if err != nil {
		return nil, err
	}
	defer releaseSnapshots()
	src, err := s.cfg.Meta.GetSandbox(ctx, sourceSandboxID)
	if err != nil {
		retErr = err
		return nil, err
	}
	var forkSnap *store.SnapshotRecord
	if src.State == store.SandboxStateRunning {
		route, rErr := s.cfg.Catalog.GetRoute(ctx, sourceSandboxID)
		if rErr != nil {
			retErr = rErr
			return nil, rErr
		}
		nodeClient, nErr := s.nodeClient(route.NodeGRPCAddr)
		if nErr != nil {
			retErr = nErr
			return nil, nErr
		}
		snapID := fmt.Sprintf("snap-fork-%s-%s", sourceSandboxID, randHex(4))
		if _, err := nodeClient.PrepareCheckpoint(ctx, &protocol.PrepareCheckpointRequest{
			SandboxID:        sourceSandboxID,
			TemplateID:       src.TemplateID,
			BaseImage:        src.BaseImage,
			SlotID:           src.CurrentSlotID,
			PodNamespace:     src.CarrierPodNamespace,
			PodName:          src.CarrierPodName,
			PodIP:            route.PodIP,
			TargetSnapshotID: snapID,
			ParentSnapshotID: src.ActiveSnapshotID,
			Kind:             protocol.SnapshotKindLiveFork,
			LeaveRunning:     true,
			BootstrapToken:   src.BootstrapToken,
		}); err != nil {
			retErr = err
			return nil, err
		}
		kubeletPort := s.lookupNodeKubeletPort(ctx, route.NodeID)
		if _, err := s.cfg.Kubelet.CheckpointContainer(ctx, route.NodeIP, kubeletPort,
			src.CarrierPodNamespace, src.CarrierPodName, protocol.ContainerNameSandbox, 60); err != nil {
			retErr = err
			return nil, err
		}
		res, gErr := nodeClient.GetLatestSnapshotResult(ctx, &protocol.GetLatestSnapshotResultRequest{SnapshotID: snapID})
		if gErr != nil || !res.Found {
			retErr = fmt.Errorf("fetch fork snapshot %s: %w", snapID, gErr)
			return nil, retErr
		}
		forkSnap = &store.SnapshotRecord{
			SnapshotID:       snapID,
			Kind:             protocol.SnapshotKindLiveFork,
			TemplateID:       src.TemplateID,
			SourceSandboxID:  sourceSandboxID,
			ParentSnapshotID: src.ActiveSnapshotID,
			BaseImage:        src.BaseImage,
			ManifestURI:      res.ManifestURI,
			OriginNodeID:     route.NodeID,
			OriginPeerAddr:   route.NodeGRPCAddr,
			TotalMemoryBytes: res.Manifest.PagesImgTotalSize,
			DirtyMemoryBytes: res.Manifest.DirtyMemoryBytes,
			DirtyChunks:      res.Manifest.DirtyChunkCount,
			ReusedChunks:     res.Manifest.ReusedChunkCount,
			BootstrapToken:   src.BootstrapToken,
			Fingerprint:      res.Manifest.Fingerprint,
		}
		if err := s.cfg.Meta.CreateSnapshot(ctx, forkSnap); err != nil {
			retErr = err
			return nil, err
		}
	} else if src.State == store.SandboxStateSuspended && src.ActiveSnapshotID != "" {
		forkSnap, err = s.cfg.Meta.GetSnapshot(ctx, src.ActiveSnapshotID)
		if err != nil {
			retErr = err
			return nil, err
		}
	} else {
		retErr = fmt.Errorf("source sandbox %s has no running state or snapshot to fork", sourceSandboxID)
		return nil, retErr
	}

	// The restored guest carries its parent's execution fence, including in
	// snapshots baked with older ktd binaries. Branch IDs have their own routes.
	branchExecution := src.ExecutionID + 1
	out := make([]*store.SandboxRecord, req.Count)
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < req.Count; i++ {
		idx := i
		g.Go(func() error {
			forkID := fmt.Sprintf("%s-fork-%d-%s", sourceSandboxID, idx+1, randHex(3))
			dec, pErr := s.cfg.Placement.SelectAndClaimSlot(gCtx, PlacementRequest{
				Tier:                        src.Tier,
				SnapshotID:                  forkSnap.SnapshotID,
				OriginNodeID:                forkSnap.OriginNodeID,
				RequiredFingerprint:         &forkSnap.Fingerprint,
				AllowFilesystemOnlyFallback: src.AllowFilesystemOnlyFallback,
			})
			if pErr != nil {
				return pErr
			}
			nc, cErr := s.nodeClient(dec.Node.GRPCAddr)
			if cErr != nil {
				return cErr
			}
			mergedEnv := make(map[string]string, len(src.EnvVars)+len(req.EnvVars))
			for k, v := range src.EnvVars {
				mergedEnv[k] = v
			}
			for k, v := range req.EnvVars {
				mergedEnv[k] = v
			}
			newTok := "kt_fork_" + randHex(16)
			lifeID := "life-" + randHex(6)
			hydResp, hErr := nc.HydrateSlot(gCtx, &protocol.HydrateSlotRequest{
				Slot:           dec.Slot,
				SandboxID:      forkID,
				TemplateID:     src.TemplateID,
				ExecutionID:    branchExecution,
				LifecycleID:    lifeID,
				SnapshotID:     forkSnap.SnapshotID,
				BaseImage:      src.BaseImage,
				RestoreMode:    dec.RestoreMode,
				BootstrapToken: forkSnap.BootstrapToken,
				AccessToken:    newTok,
				EnvVars:        mergedEnv,
				CPUMillis:      src.CPUMillis,
				MemoryBytes:    src.MemoryBytes,
			})
			if hErr != nil {
				return hErr
			}
			rec := &store.SandboxRecord{
				SandboxID:           forkID,
				TemplateID:          src.TemplateID,
				BaseImage:           src.BaseImage,
				Tier:                src.Tier,
				State:               store.SandboxStateRunning,
				ExecutionID:         branchExecution,
				LifecycleID:         lifeID,
				ActiveSnapshotID:    forkSnap.SnapshotID,
				CurrentNodeID:       dec.Node.NodeID,
				OriginNodeID:        dec.Node.NodeID,
				CurrentSlotID:       dec.Slot.SlotID,
				CarrierPodNamespace: dec.Slot.PodNamespace,
				CarrierPodName:      dec.Slot.PodName,
				PodIP:               hydResp.PodIP,
				AccessToken:         newTok,
				BootstrapToken:      forkSnap.BootstrapToken,
				EnvVars:             mergedEnv,
				IdleTimeoutSeconds:  src.IdleTimeoutSeconds,
				CPUMillis:           src.CPUMillis,
				MemoryBytes:         src.MemoryBytes,
			}
			if err := s.cfg.Meta.CreateSandbox(gCtx, rec); err != nil {
				s.abandonHydratedSlot(gCtx, dec, forkID, branchExecution)
				return err
			}
			out[idx] = rec
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		retErr = err
		return nil, err
	}
	return out, nil
}

func (s *Server) DeleteSandbox(ctx context.Context, sandboxID string) error {
	unlock, err := s.lockExecution(ctx, sandboxID)
	if err != nil {
		return err
	}
	defer unlock()
	sbx, err := s.cfg.Meta.GetSandbox(ctx, sandboxID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if sbx.CurrentSlotID != "" {
		if err := s.releaseExecution(ctx, sbx); err != nil {
			return err
		}
	}
	if sbx.ActiveSnapshotID != "" {
		if snap, sErr := s.cfg.Meta.GetSnapshot(ctx, sbx.ActiveSnapshotID); sErr == nil && snap.Kind == protocol.SnapshotKindSessionSuspend {
			_ = s.cfg.Meta.MarkSnapshotSuperseded(ctx, snap.SnapshotID)
		}
	}
	return s.cfg.Meta.DeleteSandbox(ctx, sandboxID)
}

// StartBackgroundWorkers launches the Idle Auto-Suspend Evictor and Reference-Counted Snapshot GC loops.
func (s *Server) StartBackgroundWorkers(ctx context.Context, sweepInterval time.Duration) {
	if sweepInterval <= 0 {
		sweepInterval = 5 * time.Second
	}
	go func() {
		ticker := time.NewTicker(sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.RunEvictionAndGCSweep(ctx)
			}
		}
	}()
}

func (s *Server) RunEvictionAndGCSweep(ctx context.Context) {
	release, leader, err := s.cfg.Meta.TryLeader(ctx, protocol.LeaderEvictor)
	if err != nil || !leader {
		return
	}
	defer release()

	now := time.Now().UTC()
	running, err := s.cfg.Meta.ListSandboxesToSweep(ctx)
	if err == nil {
		for _, sbx := range running {
			if sbx.State == store.SandboxStateSuspending || sbx.State == store.SandboxStateSuspended {
				_, _ = s.suspendSandbox(ctx, sbx.SandboxID, &sbx)
				continue
			}
			if sbx.IdleTimeoutSeconds <= 0 {
				continue
			}
			if n, _ := s.cfg.Catalog.Inflight(ctx, sbx.SandboxID); n > 0 {
				continue
			}
			lastActive, ok, _ := s.cfg.Catalog.GetLastActive(ctx, sbx.SandboxID)
			if !ok {
				lastActive = sbx.UpdatedAt
			}
			if now.Sub(lastActive) >= time.Duration(sbx.IdleTimeoutSeconds)*time.Second {
				_, _ = s.suspendSandbox(ctx, sbx.SandboxID, &sbx)
			}
		}
	}

	if err := s.collectSnapshots(ctx); err != nil {
		log.Printf("snapshot GC: %v", err)
	}
}

func (s *Server) collectSnapshots(ctx context.Context) error {
	if s.cfg.CAS == nil {
		return nil
	}
	candidates, err := s.cfg.Meta.ListGCandidateSnapshots(ctx)
	if err != nil || len(candidates) == 0 {
		return err
	}
	release, err := s.cfg.Meta.LockSnapshotSet(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	// A publisher may have added references while the collector waited.
	candidates, err = s.cfg.Meta.ListGCandidateSnapshots(ctx)
	if err != nil || len(candidates) == 0 {
		return err
	}
	doomed := make(map[string]bool, len(candidates))
	for _, snap := range candidates {
		doomed[snap.SnapshotID] = true
	}
	remaining, err := s.cfg.Meta.ListAllSnapshots(ctx)
	if err != nil {
		return err
	}
	live := map[string]bool{}
	for _, snap := range remaining {
		if doomed[snap.SnapshotID] {
			continue
		}
		mf, err := s.gcManifest(ctx, snap)
		if err != nil {
			return err
		}
		for _, ch := range mf.Chunks {
			if !ch.ZeroChunk {
				live[ch.Blake3Hash] = true
			}
		}
	}
	for _, snap := range candidates {
		mf, err := s.gcManifest(ctx, snap)
		if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
			return err
		}
		if mf != nil {
			for _, ch := range mf.Chunks {
				if ch.ZeroChunk || live[ch.Blake3Hash] {
					continue
				}
				if err := s.cfg.CAS.DeleteBlob(ctx, storage.ChunkKey(ch.Blake3Hash)); err != nil {
					return err
				}
			}
		}
		// Deletions are idempotent. Keep the manifest and candidate row until
		// their dependent objects are gone so an interrupted sweep can retry.
		for _, name := range []string{protocol.GVisorCheckpointImg, protocol.GVisorPagesMetaImg, protocol.RootfsUpperTar, protocol.ManifestFileName} {
			if err := s.cfg.CAS.DeleteBlob(ctx, storage.ArtifactKey(snap.SnapshotID, name)); err != nil {
				return err
			}
		}
		if err := s.cfg.Meta.DeleteSnapshot(ctx, snap.SnapshotID); err != nil {
			return err
		}
	}
	if local, ok := s.cfg.CAS.(*storage.LocalFSBackend); ok {
		hashes, err := local.ListLocalChunkHashes()
		if err != nil {
			return err
		}
		for _, h := range hashes {
			if !live[h] {
				if err := local.DeleteBlob(ctx, storage.ChunkKey(h)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Server) gcManifest(ctx context.Context, snap store.SnapshotRecord) (*protocol.SnapshotManifest, error) {
	mf, err := s.cfg.CAS.GetManifest(ctx, snap.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", snap.SnapshotID, err)
	}
	if mf == nil || mf.SnapshotID != snap.SnapshotID || mf.PagesImgTotalSize != snap.TotalMemoryBytes {
		return nil, fmt.Errorf("invalid manifest for %s", snap.SnapshotID)
	}
	var size int64
	for _, ch := range mf.Chunks {
		if ch.Offset != size || ch.Length <= 0 || (!ch.ZeroChunk && ch.Blake3Hash == "") {
			return nil, fmt.Errorf("invalid chunk in manifest %s", snap.SnapshotID)
		}
		size += int64(ch.Length)
	}
	if size != mf.PagesImgTotalSize {
		return nil, fmt.Errorf("incomplete manifest %s", snap.SnapshotID)
	}
	return mf, nil
}

func (s *Server) broadcastL1Prewarm(ctx context.Context, snapshotID string) {
	nodes, err := s.cfg.Catalog.ListHealthyNodes(ctx)
	if err != nil {
		return
	}
	for _, n := range nodes {
		if nc, cErr := s.nodeClient(n.GRPCAddr); cErr == nil {
			_, _ = nc.PrewarmTemplate(ctx, &protocol.PrewarmTemplateRequest{
				SnapshotID: snapshotID,
				PinInTmpfs: true,
			})
		}
	}
}

func (s *Server) lookupNodeKubeletPort(ctx context.Context, nodeID string) int {
	nodes, _ := s.cfg.Catalog.ListHealthyNodes(ctx)
	for _, n := range nodes {
		if n.NodeID == nodeID && n.KubeletPort > 0 {
			return n.KubeletPort
		}
	}
	return 10250
}

func (s *Server) nodeClient(addr string) (*protocol.NodeControlClient, error) {
	s.nodeConnMu.Lock()
	defer s.nodeConnMu.Unlock()
	if cc, ok := s.nodeConns[addr]; ok {
		return protocol.NewNodeControlClient(cc), nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(telemetry.UnaryClientInterceptor("kontinue-apiserver")),
	)
	if err != nil {
		return nil, err
	}
	s.nodeConns[addr] = cc
	return protocol.NewNodeControlClient(cc), nil
}

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func redactSandbox(sbx *store.SandboxRecord) store.SandboxRecord {
	if sbx == nil {
		return store.SandboxRecord{}
	}
	cp := *sbx
	cp.BootstrapToken = ""
	return cp
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
