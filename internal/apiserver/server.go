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
	RequestKey string            `json:"-"`
	Count      int               `json:"count"`
	EnvVars    map[string]string `json:"envVars,omitempty"`
}

func (s *Server) handleTemplates(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		list, err := s.cfg.Meta.ListTemplates(r.Context())
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		public := make([]templateResponse, 0, len(list))
		for i := range list {
			public = append(public, publicTemplate(&list[i]))
		}
		writeJSON(w, http.StatusOK, public)
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
		writeJSON(w, http.StatusCreated, publicTemplate(tpl))
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
	writeJSON(w, http.StatusOK, publicTemplate(tpl))
}

func (s *Server) handleSandboxes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	idemKey := r.Header.Get(protocol.HeaderIdempotencyKey)
	if idemKey != "" {
		idemKey = "create-v2/" + idemKey
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
	raw, _ := json.Marshal(issueSandbox(sbx))
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
			writeJSON(w, http.StatusOK, publicSandbox(sbx))
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
		writeJSON(w, http.StatusOK, publicSnapshot(snap))
	case "resume":
		sbx, err := s.ResumeSandbox(r.Context(), sandboxID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, publicSandbox(sbx))
	case "fork":
		var fReq ForkSandboxRequest
		if r.Body != nil && r.ContentLength != 0 {
			if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&fReq); err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		fReq.RequestKey = r.Header.Get(protocol.HeaderIdempotencyKey)
		forks, err := s.ForkSandbox(r.Context(), sandboxID, fReq)
		if err != nil {
			if errors.Is(err, store.ErrTransitionLocked) || errors.Is(err, store.ErrStaleExecutionFence) {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		pub := make([]issuedSandbox, 0, len(forks))
		for _, f := range forks {
			pub = append(pub, issueSandbox(f))
		}
		writeJSON(w, http.StatusCreated, pub)
	default:
		writeErr(w, http.StatusNotFound, "unknown sandbox action")
	}
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
		if tpl.Phase != "Ready" || tpl.GoldenSnapshotID == "" {
			return nil, store.ErrTransitionLocked
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

	var snapID, bootTok string
	if snap != nil {
		snapID = snap.SnapshotID
		bootTok = snap.BootstrapToken
	}
	if bootTok == "" {
		bootTok = "boot-" + randHex(12)
	}
	sbx := &store.SandboxRecord{SandboxID: req.SandboxID, TemplateID: req.TemplateID, BaseImage: baseImage, Tier: tier,
		State: store.SandboxStateCreating, ExecutionID: 1, LifecycleID: "life-" + randHex(6), ActiveSnapshotID: snapID,
		AccessToken: "kt_live_" + randHex(16), BootstrapToken: bootTok, EnvVars: req.EnvVars, IdleTimeoutSeconds: req.IdleTimeoutSeconds,
		CPUMillis: req.CPUMillis, MemoryBytes: req.MemoryBytes, AllowFilesystemOnlyFallback: req.AllowFilesystemOnlyFallback}
	if err := s.cfg.Meta.CreateSandbox(ctx, sbx); err != nil {
		return nil, err
	}
	return s.advanceActivation(ctx, sbx)
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
	if sbx.PendingFork != nil {
		return nil, store.ErrTransitionLocked
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
		snapRec, err = s.captureSandbox(ctx, sbx, route, sbx.PendingSnapshotID, protocol.SnapshotKindSessionSuspend)
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
	released, err := nc.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: sbx.CurrentSlotID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, LifecycleID: sbx.LifecycleID, ReuseSlot: s.cfg.ReuseSlots})
	if err != nil {
		return err
	}
	if !released.Released {
		return fmt.Errorf("slot %s release is unconfirmed", sbx.CurrentSlotID)
	}
	if !s.cfg.ReuseSlots && s.cfg.Replenisher != nil {
		s.cfg.Replenisher.EnqueueTaintedPod(sbx.CarrierPodNamespace, sbx.CarrierPodName, released.PodUID)
	}
	return nil
}

func (s *Server) captureSandbox(ctx context.Context, sbx *store.SandboxRecord, route *protocol.RouteRecord, newSnapID string, kind protocol.SnapshotKind) (*store.SnapshotRecord, error) {
	prep := &protocol.PrepareCheckpointRequest{
		SandboxID:        sbx.SandboxID,
		ExecutionID:      sbx.ExecutionID,
		LifecycleID:      sbx.LifecycleID,
		TemplateID:       sbx.TemplateID,
		BaseImage:        sbx.BaseImage,
		SlotID:           sbx.CurrentSlotID,
		PodNamespace:     sbx.CarrierPodNamespace,
		PodName:          sbx.CarrierPodName,
		PodIP:            route.PodIP,
		TargetSnapshotID: newSnapID,
		ParentSnapshotID: sbx.ActiveSnapshotID,
		Kind:             kind,
		LeaveRunning:     true,
		BootstrapToken:   sbx.BootstrapToken,
	}
	return s.publishCheckpoint(ctx, prep, route.NodeID, route.NodeIP, route.NodeGRPCAddr, route.NodeGRPCAddr)
}

func (s *Server) publishCheckpoint(ctx context.Context, prep *protocol.PrepareCheckpointRequest, nodeID, nodeIP, grpcAddr, peerAddr string) (*store.SnapshotRecord, error) {
	releaseSnapshots, err := s.cfg.Meta.LockSnapshotSet(ctx, false)
	if err != nil {
		return nil, err
	}
	defer releaseSnapshots()
	nodeClient, err := s.nodeClient(grpcAddr)
	if err != nil {
		return nil, err
	}
	newSnapID := prep.TargetSnapshotID
	query := &protocol.GetLatestSnapshotResultRequest{SnapshotID: newSnapID, Expected: prep}
	snapRes, err := nodeClient.GetLatestSnapshotResult(ctx, query)
	if err != nil {
		return nil, err
	}
	if !snapRes.Found {
		if _, err := nodeClient.PrepareCheckpoint(ctx, prep); err != nil {
			return nil, err
		}

		kubeletPort := s.lookupNodeKubeletPort(ctx, nodeID)
		if _, err := s.cfg.Kubelet.CheckpointContainer(ctx, nodeIP, kubeletPort,
			prep.PodNamespace, prep.PodName, protocol.ContainerNameSandbox, 60); err != nil {
			return nil, err
		}

		snapRes, err = nodeClient.GetLatestSnapshotResult(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("fetch snapshot %s: %w", newSnapID, err)
		}
		if !snapRes.Found {
			return nil, fmt.Errorf("snapshot %s is unavailable", newSnapID)
		}

	}

	snapRec := &store.SnapshotRecord{
		SnapshotID:       newSnapID,
		Kind:             prep.Kind,
		TemplateID:       prep.TemplateID,
		SourceSandboxID:  prep.SandboxID,
		ParentSnapshotID: prep.ParentSnapshotID,
		BaseImage:        prep.BaseImage,
		ManifestURI:      snapRes.ManifestURI,
		OriginNodeID:     nodeID,
		OriginPeerAddr:   peerAddr,
		TotalMemoryBytes: snapRes.Manifest.PagesImgTotalSize,
		DirtyMemoryBytes: snapRes.Manifest.DirtyMemoryBytes,
		DirtyChunks:      snapRes.Manifest.DirtyChunkCount,
		ReusedChunks:     snapRes.Manifest.ReusedChunkCount,
		BootstrapToken:   prep.BootstrapToken,
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
			r.OwnerKey = store.OwnerKey(*r)
			if err := s.cfg.Catalog.PublishRoute(ctx, *r); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		return sbx, nil
	}

	if sbx.State == store.SandboxStateCreating || sbx.State == store.SandboxStateResuming {
		return s.advanceActivation(ctx, sbx)
	}
	if sbx.State != store.SandboxStateSuspended || sbx.PendingFork != nil {
		return nil, store.ErrTransitionLocked
	}
	if err := s.finishSuspendRelease(ctx, sbx); err != nil {
		return nil, err
	}

	sbx.State = store.SandboxStateResuming
	sbx.ExecutionID++
	sbx.LifecycleID = "life-" + randHex(6)
	if err := s.cfg.Meta.SaveSandbox(ctx, sbx); err != nil {
		return nil, err
	}
	return s.advanceActivation(ctx, sbx)
}

func (s *Server) routeForExecution(ctx context.Context, sbx *store.SandboxRecord) (*protocol.RouteRecord, error) {
	if existing, err := s.cfg.Catalog.GetRoute(ctx, sbx.SandboxID); err == nil {
		if existing.ExecutionID != sbx.ExecutionID || existing.SlotID != sbx.CurrentSlotID || existing.LifecycleID != sbx.LifecycleID || existing.NodeID != sbx.CurrentNodeID {
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
	if sbx.PendingFork != nil && sbx.State != store.SandboxStateFailed {
		return store.ErrTransitionLocked
	}
	if sbx.State == store.SandboxStateCreating || sbx.State == store.SandboxStateResuming {
		if err := s.cfg.Catalog.FinishReservation(ctx, sbx.LifecycleID, sbx.PendingPlacement == nil); err != nil {
			return err
		}
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

	s.reconcileTemplateBuilds(ctx)
	now := time.Now().UTC()
	running, err := s.cfg.Meta.ListSandboxesToSweep(ctx)
	if err == nil {
		for _, sbx := range running {
			if sbx.State == store.SandboxStateResuming || sbx.State == store.SandboxStateCreating {
				_, _ = s.resumePending(ctx, &sbx)
				continue
			}
			if s.reconcileSandbox(ctx, &sbx) {
				continue
			}
			if sbx.PendingFork != nil {
				_, _ = s.prepareFork(ctx, sbx.SandboxID, nil, sbx.PendingFork.OperationID)
				continue
			}
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

func (s *Server) reconcileSandbox(ctx context.Context, observed *store.SandboxRecord) bool {
	if observed.CurrentSlotID == "" || (observed.State != store.SandboxStateRunning && observed.State != store.SandboxStateSuspending) {
		return false
	}
	r := protocol.RouteRecord{SandboxID: observed.SandboxID, SlotID: observed.CurrentSlotID, ExecutionID: observed.ExecutionID, LifecycleID: observed.LifecycleID, NodeID: observed.CurrentNodeID}
	report, err := s.cfg.Catalog.ExecutionReport(ctx, r)
	if err != nil {
		return false
	}
	if report.State == store.OwnerReady {
		if observed.State == store.SandboxStateRunning {
			if _, err := s.cfg.Catalog.GetRoute(ctx, observed.SandboxID); errors.Is(err, store.ErrNotFound) {
				_, _ = s.ResumeSandbox(ctx, observed.SandboxID)
			}
		}
		return false
	}
	if report.State != store.OwnerFailed {
		return true
	}
	unlock, err := s.lockExecution(ctx, observed.SandboxID)
	if err != nil {
		return true
	}
	defer unlock()
	sb, err := s.cfg.Meta.GetSandbox(ctx, observed.SandboxID)
	if err != nil || sb.ExecutionID != r.ExecutionID || sb.LifecycleID != r.LifecycleID || sb.CurrentSlotID != r.SlotID || sb.CurrentNodeID != r.NodeID || sb.State != observed.State {
		return true
	}
	if sb.State == store.SandboxStateSuspending && sb.PendingSnapshotID != "" {
		// A snapshot committed before the crash can still finish its normal release.
		if _, err := s.cfg.Meta.GetSnapshot(ctx, sb.PendingSnapshotID); err == nil {
			return false
		} else if !errors.Is(err, store.ErrNotFound) {
			return true
		}
	}
	sb.State, sb.FailureReason = store.SandboxStateFailed, report.Reason
	if err := s.cfg.Meta.SaveSandbox(ctx, sb); err != nil {
		log.Printf("persist recovery failure for %s: %v", sb.SandboxID, err)
	}
	return true
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
	if pending, err := s.cfg.Meta.UnpublishedSnapshot(ctx); err != nil {
		return err
	} else if pending != "" {
		return fmt.Errorf("snapshot %s awaits publication", pending)
	}
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
