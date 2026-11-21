package arena

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
	"lukechampine.com/blake3"
)

const (
	TierL1Tmpfs        = "L1_TMPFS"
	TierL2NVMe         = "L2_NVME"
	TierL3ArenaFS      = "L3_ARENAFS"
	TierL3Materialized = "L3_MATERIALIZED"
)

type ViewCloser interface {
	Close() error
}

type plainDirHandle struct {
	dir    string
	reader *LazyPageReader
}

func (h *plainDirHandle) Close() error {
	if h.reader != nil {
		if err := h.reader.Close(); err != nil {
			return err
		}
	}
	if h.dir != "" {
		return os.RemoveAll(h.dir)
	}
	return nil
}

func fallbackMaterializeDir(ctx context.Context, dir string, ckptBytes, metaBytes []byte, reader *LazyPageReader) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if len(ckptBytes) > 0 {
		if err := os.WriteFile(filepath.Join(dir, protocol.GVisorCheckpointImg), ckptBytes, 0o600); err != nil {
			return err
		}
	}
	if len(metaBytes) > 0 {
		if err := os.WriteFile(filepath.Join(dir, protocol.GVisorPagesMetaImg), metaBytes, 0o600); err != nil {
			return err
		}
	}
	if reader != nil {
		reader.copiedBytes.Add(int64(len(ckptBytes) + len(metaBytes)))
		if err := reader.WarmAll(ctx); err != nil {
			return err
		}
		dst := filepath.Join(dir, protocol.GVisorPagesImg)
		src := reader.sparseFile.Name()
		if os.Link(src, dst) != nil {
			in, err := os.Open(src)
			if err != nil {
				return err
			}
			defer in.Close()
			out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return err
			}
			n, copyErr := io.Copy(out, in)
			reader.copiedBytes.Add(n)
			err = errors.Join(copyErr, out.Close())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type ManagerConfig struct {
	NodeID          string
	P2PAddr         string
	L1TmpfsDir      string
	L2CASDir        string
	ViewsDir        string
	HooksSockPath   string
	RustArenaHelper string
	RemoteStore     storage.Backend
	Recorder        *telemetry.FlightRecorder
}

// Manager orchestrates the 3-Tier Kontinue Memory Arena (L1 tmpfs, L2 NVMe 2MiB Blake3 CAS, and L3 ArenaFS FUSE + P2P).
type Manager struct {
	cfg ManagerConfig

	releases    singleflight.Group
	pins        singleflight.Group
	mu          sync.RWMutex
	l1Pinned    map[string]string
	l2Manifests map[string]*protocol.SnapshotManifest
	activeViews map[string]ViewCloser

	hookSrv  *http.Server
	uploadWG sync.WaitGroup
}

func NewManager(cfg ManagerConfig) (*Manager, error) {
	for _, d := range []string{cfg.L1TmpfsDir, cfg.L2CASDir, cfg.ViewsDir} {
		if d != "" {
			if err := os.MkdirAll(d, 0o700); err != nil {
				return nil, fmt.Errorf("mkdir %s: %w", d, err)
			}
		}
	}
	m := &Manager{
		cfg:         cfg,
		l1Pinned:    make(map[string]string),
		l2Manifests: make(map[string]*protocol.SnapshotManifest),
		activeViews: make(map[string]ViewCloser),
	}
	if cfg.HooksSockPath != "" {
		if err := m.startHooksUnixServer(cfg.HooksSockPath); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) startHooksUnixServer(sockPath string) error {
	_ = os.MkdirAll(filepath.Dir(sockPath), 0o700)
	_ = os.Remove(sockPath)
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/arena/release-view", func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ContainerID string `json:"containerId"`
			ViewPath    string `json:"viewPath"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// gVisor reports restore complete before it has read every page. Unmounting
		// here drops the pages file while the guest can still fault the rest, so
		// the view stays until ReleaseSlot.
		w.WriteHeader(http.StatusNoContent)
	})
	if m.cfg.Recorder != nil {
		mux.HandleFunc(protocol.PathDebugTraces, m.cfg.Recorder.Handler())
	}
	m.hookSrv = &http.Server{Handler: mux}
	go func() { _ = m.hookSrv.Serve(lis) }()
	return nil
}

func (m *Manager) Inventory() (l1 []string, l2 []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k := range m.l1Pinned {
		l1 = append(l1, k)
	}
	for k := range m.l2Manifests {
		l2 = append(l2, k)
	}
	return l1, l2
}

func (m *Manager) LoadLocalManifest(snapshotID string) (*protocol.SnapshotManifest, error) {
	m.mu.RLock()
	if cached, ok := m.l2Manifests[snapshotID]; ok {
		m.mu.RUnlock()
		return cached, nil
	}
	m.mu.RUnlock()

	p := filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(snapshotID, protocol.ManifestFileName))
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var mf protocol.SnapshotManifest
	if err := json.Unmarshal(raw, &mf); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.l2Manifests[snapshotID] = &mf
	m.mu.Unlock()
	return &mf, nil
}

func (m *Manager) ReadLocalChunk(blake3Hash string) ([]byte, error) {
	return os.ReadFile(filepath.Join(m.cfg.L2CASDir, storage.ChunkKey(blake3Hash)))
}

type IngestOptions struct {
	BeforePublish    func(*protocol.SnapshotManifest, string) error
	SnapshotID       string
	Kind             protocol.SnapshotKind
	TemplateID       string
	BaseImage        string
	ParentSnapshotID string
	BootstrapToken   string
	RawCheckpointDir string
	KubeletTarPath   string
	PinInL1Tmpfs     bool
	TraceHotPages    bool
	Fingerprint      protocol.CompatibilityFingerprint
}

// IngestRawCheckpoint chunks a newly captured gVisor checkpoint directory via Rust kontinue-arena-helper
// (or built-in Blake3 chunker), deduplicates against ParentSnapshotID, stages Kubelet's descriptor tar,
// optionally pins Golden Templates in L1 tmpfs, and uploads dirty chunks to RemoteStore.
func (m *Manager) IngestRawCheckpoint(ctx context.Context, opts IngestOptions) (*protocol.SnapshotManifest, string, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-arena", "arena.ingest_checkpoint",
		attribute.String(telemetry.AttrSnapshotID, opts.SnapshotID),
		attribute.String("parent.snapshot_id", opts.ParentSnapshotID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	var parentManifest *protocol.SnapshotManifest
	var parentPath string
	if opts.ParentSnapshotID != "" {
		if pm, err := m.ResolveManifest(ctx, opts.ParentSnapshotID); err == nil {
			parentManifest = pm
			parentPath = filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(opts.ParentSnapshotID, protocol.ManifestFileName))
			if _, statErr := os.Stat(parentPath); statErr != nil {
				rawPM, _ := json.Marshal(pm)
				_ = writeAtomicFile(parentPath, rawPM)
			}
		}
	}

	outManifestPath := filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(opts.SnapshotID, protocol.ManifestFileName))
	var manifest *protocol.SnapshotManifest

	if m.cfg.RustArenaHelper != "" {
		if _, err := os.Stat(m.cfg.RustArenaHelper); err == nil {
			draftPath := filepath.Join(opts.RawCheckpointDir, protocol.ManifestFileName)
			fpJSON, _ := json.Marshal(opts.Fingerprint)
			args := []string{
				"chunk-diff",
				"--snapshot-id", opts.SnapshotID,
				"--kind", string(opts.Kind),
				"--template-id", opts.TemplateID,
				"--base-image", opts.BaseImage,
				"--origin-node", m.cfg.NodeID,
				"--origin-peer", m.cfg.P2PAddr,
				"--bootstrap-token", opts.BootstrapToken,
				"--input-dir", opts.RawCheckpointDir,
				"--cas-dir", m.cfg.L2CASDir,
				"--output-manifest", draftPath,
				"--fingerprint-json", string(fpJSON),
			}
			if parentPath != "" {
				args = append(args, "--parent-manifest", parentPath)
			}
			cmd := exec.CommandContext(ctx, m.cfg.RustArenaHelper, args...)
			cmd.Env = telemetry.InjectEnv(ctx, os.Environ())
			if out, runErr := cmd.CombinedOutput(); runErr != nil {
				retErr = fmt.Errorf("kontinue-arena-helper chunk-diff failed: %w (%s)", runErr, string(out))
				return nil, "", retErr
			}
			raw, err := os.ReadFile(draftPath)
			if err != nil {
				retErr = err
				return nil, "", err
			}
			manifest = new(protocol.SnapshotManifest)
			if err := json.Unmarshal(raw, manifest); err != nil {
				retErr = err
				return nil, "", err
			}
		}
	}

	if manifest == nil {
		var err error
		manifest, err = m.chunkAndDiffGo(opts, parentManifest)
		if err != nil {
			retErr = err
			return nil, "", err
		}
	}

	// HotPageIndices is filled later from the read order gVisor actually issues
	// against the ArenaFS pages file. A build-time scan cannot see that order.

	manifestURI := m.checkpointManifestURI(opts.SnapshotID)
	if opts.BeforePublish != nil {
		if err := opts.BeforePublish(manifest, manifestURI); err != nil {
			retErr = err
			return nil, "", err
		}
	}
	if err := m.publishCheckpointManifest(ctx, manifest); err != nil {
		retErr = err
		return nil, "", err
	}

	span.SetAttributes(
		attribute.Int("dirty_chunks_count", int(manifest.DirtyChunkCount)),
		attribute.Int("reused_chunks_count", int(manifest.ReusedChunkCount)),
		attribute.Int64(telemetry.AttrBytes, manifest.DirtyMemoryBytes),
	)

	if opts.KubeletTarPath != "" {
		if err := writeKubeletCheckpointTar(opts.KubeletTarPath, manifest, manifestURI, outManifestPath); err != nil {
			retErr = err
			return nil, "", err
		}
	}

	if opts.PinInL1Tmpfs || opts.Kind == protocol.SnapshotKindGoldenTemplate {
		_, _ = m.PrewarmToL1Tmpfs(ctx, manifest)
	}

	return manifest, manifestURI, nil
}

// probeSkipThawHotPages faults pages through LazyPageReader in a restore-like order:
// the highest non-zero chunk first (guest stack), then the remaining pages low-to-high.
// That matches a skipThaw empty resume better than "append every non-zero chunk index".
func (m *Manager) probeSkipThawHotPages(ctx context.Context, manifest *protocol.SnapshotManifest) ([]uint32, error) {
	backing := filepath.Join(m.cfg.L2CASDir, "staging", manifest.SnapshotID+"-hotprobe.pages")
	reader, err := NewLazyPageReader(LazyReaderOptions{
		Manifest:    manifest,
		LocalCASDir: m.cfg.L2CASDir,
		BackingFile: backing,
		RemoteStore: m.cfg.RemoteStore,
		TraceAccess: true,
	})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	_ = os.Remove(backing + ".tmp")

	order := skipThawTouchOrder(manifest.Chunks)
	buf := make([]byte, 4096)
	for _, ch := range order {
		_, _ = reader.ReadAt(ctx, buf, ch.Offset)
	}
	return reader.RecordedHotPages(), nil
}

func skipThawTouchOrder(chunks []protocol.ChunkRef) []protocol.ChunkRef {
	var nz []protocol.ChunkRef
	for _, ch := range chunks {
		if ch.ZeroChunk {
			continue
		}
		nz = append(nz, ch)
	}
	if len(nz) <= 1 {
		return nz
	}
	out := make([]protocol.ChunkRef, 0, len(nz))
	out = append(out, nz[len(nz)-1])
	out = append(out, nz[:len(nz)-1]...)
	return out
}

func (m *Manager) uploadSnapshotToRemote(ctx context.Context, mf *protocol.SnapshotManifest) (uri string, err error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-arena", "arena.upload_snapshot", attribute.String(telemetry.AttrSnapshotID, mf.SnapshotID))
	defer func() { telemetry.EndSpan(span, err) }()
	for _, art := range checkpointArtifacts(mf) {
		data, err := os.ReadFile(filepath.Join(m.cfg.L2CASDir, art.key))
		if err != nil {
			return "", err
		}
		if err := m.cfg.RemoteStore.PutBlob(ctx, art.key, bytes.NewReader(data), int64(len(data))); err != nil {
			return "", err
		}
	}

	for _, ch := range mf.Chunks {
		if ch.ZeroChunk || ch.Blake3Hash == "" {
			continue
		}
		key := storage.ChunkKey(ch.Blake3Hash)
		if ch.ReusedFromParent {
			if exists, err := m.cfg.RemoteStore.HasBlob(ctx, key); err == nil && exists {
				continue
			}
		}
		data, err := os.ReadFile(filepath.Join(m.cfg.L2CASDir, key))
		if err != nil {
			return "", err
		}
		if err := m.cfg.RemoteStore.PutBlob(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			return "", err
		}
	}
	return m.cfg.RemoteStore.PutManifest(ctx, mf.SnapshotID, mf)
}

func (m *Manager) ResolveManifest(ctx context.Context, snapshotID string) (*protocol.SnapshotManifest, error) {
	if mf, err := m.LoadLocalManifest(snapshotID); err == nil {
		return mf, nil
	}
	if m.cfg.RemoteStore != nil {
		mf, err := m.cfg.RemoteStore.GetManifest(ctx, snapshotID)
		if err != nil {
			return nil, err
		}
		raw, _ := json.MarshalIndent(mf, "", "  ")
		_ = writeAtomicFile(filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(snapshotID, protocol.ManifestFileName)), raw)
		m.mu.Lock()
		m.l2Manifests[snapshotID] = mf
		m.mu.Unlock()
		return mf, nil
	}
	return nil, fmt.Errorf("snapshot %s manifest not found", snapshotID)
}

func (m *Manager) PrewarmToL1Tmpfs(ctx context.Context, mf *protocol.SnapshotManifest) (path string, err error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-arena", "arena.prewarm_l1_tmpfs",
		attribute.String(telemetry.AttrSnapshotID, mf.SnapshotID),
	)
	defer func() { telemetry.EndSpan(span, err) }()

	key := pinPath(m.cfg.L1TmpfsDir, mf)
	value, err, _ := m.pins.Do(key, func() (any, error) {
		if _, err := os.Stat(key); errors.Is(err, os.ErrNotExist) {
			tmp, err := os.MkdirTemp(m.cfg.L1TmpfsDir, ".pin-")
			if err != nil {
				return "", err
			}
			defer os.RemoveAll(tmp)
			ckpt, meta, err := m.loadCoreArtifacts(ctx, mf)
			if err != nil {
				return "", err
			}
			// Place the sparse backing file on tmpfs so materialization can link it.
			reader, err := NewLazyPageReader(LazyReaderOptions{Manifest: mf, LocalCASDir: m.cfg.L2CASDir, BackingFile: filepath.Join(tmp, "backing"), RemoteStore: m.cfg.RemoteStore, OriginPeerAddr: mf.OriginPeerAddr})
			if err != nil {
				return "", err
			}
			err = fallbackMaterializeDir(ctx, tmp, ckpt, meta, reader)
			err = errors.Join(err, reader.Close())
			if err != nil {
				return "", err
			}
			if err = os.Rename(tmp, key); err != nil {
				return "", err
			}
		} else if err != nil {
			return "", err
		}
		m.mu.Lock()
		m.l1Pinned[mf.SnapshotID] = key
		m.mu.Unlock()
		return key, nil
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func pinPath(root string, mf *protocol.SnapshotManifest) string {
	// A capture retried before publication can reuse its ID with a new image.
	return filepath.Join(root, fmt.Sprintf("%s-%d", mf.SnapshotID, mf.CreatedAt.UnixNano()))
}

// PrepareRestoreView selects the fastest Arena Tier available for restoring `snapshotID`:
// 1. L1_TMPFS: zero-copy hardlink view into host RAM tmpfs;
// 2. L2_NVME: local CAS materialization when all chunks exist on local NVMe;
// 3. L3_ARENAFS: instant FUSE mount backed by LazyPageReader (P2P + S3 Range + Hot-Page Prefetcher)!
func (m *Manager) PrepareRestoreView(ctx context.Context, snapshotID, viewID string) (viewPath string, tier string, mf *protocol.SnapshotManifest, copiedBytes int64, err error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-arena", "arena.prepare_restore_view",
		attribute.String(telemetry.AttrSnapshotID, snapshotID),
		attribute.String("view.id", viewID),
	)
	defer func() {
		if tier != "" {
			span.SetAttributes(attribute.String(telemetry.AttrArenaTier, tier), attribute.Int64("restore.view_copied_bytes", copiedBytes))
		}
		telemetry.EndSpan(span, err)
	}()
	var createdViewPath string
	var retainedView bool
	defer func() {
		if err != nil && createdViewPath != "" && !retainedView {
			_ = m.ReleaseView(createdViewPath)
			_ = os.RemoveAll(createdViewPath)
		}
	}()

	mf, err = m.ResolveManifest(ctx, snapshotID)
	if err != nil {
		return "", "", nil, 0, err
	}

	if !filepath.IsLocal(viewID) || filepath.Base(viewID) != viewID || viewID == "." {
		return "", "", nil, 0, fmt.Errorf("invalid view ID %q", viewID)
	}
	pin := pinPath(m.cfg.L1TmpfsDir, mf)
	if info, statErr := os.Stat(pin); statErr == nil && info.IsDir() {
		viewDir := filepath.Join(m.cfg.L1TmpfsDir, ".views", viewID)
		if err := os.MkdirAll(filepath.Dir(viewDir), 0700); err != nil {
			return "", "", nil, 0, err
		}
		if err := os.Mkdir(viewDir, 0700); err != nil {
			return "", "", nil, 0, err
		}
		createdViewPath = viewDir
		if err := cloneDirHardlinks(pin, viewDir); err == nil {
			m.trackView(viewDir, &plainDirHandle{dir: viewDir})
			retainedView = true
			m.mu.Lock()
			m.l1Pinned[snapshotID] = pin
			m.mu.Unlock()
			return viewDir, TierL1Tmpfs, mf, 0, nil
		}
		if err := os.RemoveAll(viewDir); err != nil {
			return "", "", nil, 0, err
		}
	}
	viewDir := filepath.Join(m.cfg.ViewsDir, viewID)
	if err = os.Mkdir(viewDir, 0700); err != nil {
		return "", "", nil, 0, err
	}
	createdViewPath = viewDir

	// Check Tier 2: Are all non-zero chunks already cached in Local L2 CAS?
	allLocal := true
	for _, ch := range mf.Chunks {
		if ch.ZeroChunk || ch.Blake3Hash == "" {
			continue
		}
		if _, statErr := os.Stat(filepath.Join(m.cfg.L2CASDir, storage.ChunkKey(ch.Blake3Hash))); statErr != nil {
			allLocal = false
			break
		}
	}

	ckptBytes, metaBytes, loadErr := m.loadCoreArtifacts(ctx, mf)
	if loadErr != nil {
		return "", "", nil, 0, loadErr
	}

	stagingPages := filepath.Join(m.cfg.L2CASDir, "staging", viewID, "backing.pages")
	peerAddr := mf.OriginPeerAddr
	if mf.OriginNodeID == m.cfg.NodeID {
		peerAddr = ""
	}
	reader, rErr := NewLazyPageReader(LazyReaderOptions{
		Manifest:       mf,
		LocalCASDir:    m.cfg.L2CASDir,
		BackingFile:    stagingPages,
		RemoteStore:    m.cfg.RemoteStore,
		OriginPeerAddr: peerAddr,
		TraceAccess:    true,
	})
	if rErr != nil {
		return "", "", nil, 0, rErr
	}

	if allLocal {
		if err = fallbackMaterializeDir(ctx, viewDir, ckptBytes, metaBytes, reader); err != nil {
			_ = reader.Close()
			return "", "", nil, 0, err
		}
		_ = reader.Close()
		m.trackView(viewDir, &plainDirHandle{dir: viewDir})
		retainedView = true
		return viewDir, TierL2NVMe, mf, reader.copiedBytes.Load(), nil
	}

	// Tier 3: Cross-node / cold-cache L3 ArenaFS FUSE lazy mount + Hot-Page Prefetcher!
	closer, mErr := mountArenaFSView(ctx, viewDir, ckptBytes, metaBytes, reader)
	if mErr != nil {
		_ = reader.Close()
		return "", "", nil, 0, mErr
	}
	m.trackView(viewDir, closer)
	retainedView = true
	if _, plain := closer.(*plainDirHandle); plain {
		return viewDir, TierL3Materialized, mf, reader.copiedBytes.Load(), nil
	}
	return viewDir, TierL3ArenaFS, mf, reader.copiedBytes.Load(), nil
}

type faultSource interface {
	RecordedFaults() []uint32
	SnapshotID() string
}

func (m *Manager) ReleaseView(viewPath string) error {
	_, err, _ := m.releases.Do(viewPath, func() (any, error) { return nil, m.closeView(viewPath) })
	return err
}

func (m *Manager) closeView(viewPath string) error {
	m.mu.RLock()
	closer, ok := m.activeViews[viewPath]
	m.mu.RUnlock()
	if !ok || closer == nil {
		return nil
	}
	var faults []uint32
	var snapshotID string
	if src, ok := closer.(faultSource); ok {
		faults = src.RecordedFaults()
		snapshotID = src.SnapshotID()
	}
	if err := closer.Close(); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.activeViews, viewPath)
	m.mu.Unlock()
	if snapshotID != "" && len(faults) > 0 {
		m.persistFaultOrder(snapshotID, faults)
	}
	return nil
}

func (m *Manager) persistFaultOrder(snapshotID string, faults []uint32) {
	mf, err := m.ResolveManifest(context.Background(), snapshotID)
	if err != nil || mf == nil || sameU32(mf.HotPageIndices, faults) {
		return
	}
	updated := *mf
	updated.HotPageIndices = append([]uint32(nil), faults...)
	raw, err := json.MarshalIndent(&updated, "", "  ")
	if err != nil {
		return
	}
	path := filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(snapshotID, protocol.ManifestFileName))
	_ = writeAtomicFile(path, raw)
	m.mu.Lock()
	m.l2Manifests[snapshotID] = &updated
	m.mu.Unlock()
	if m.cfg.RemoteStore != nil {
		_, _ = m.cfg.RemoteStore.PutManifest(context.Background(), snapshotID, &updated)
	}
}

func sameU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (m *Manager) Shutdown() {
	if m.hookSrv != nil {
		_ = m.hookSrv.Close()
	}
	m.uploadWG.Wait()
}

func (m *Manager) Close() error {
	m.Shutdown()
	m.mu.RLock()
	paths := make([]string, 0, len(m.activeViews))
	for path := range m.activeViews {
		paths = append(paths, path)
	}
	m.mu.RUnlock()
	var err error
	for _, path := range paths {
		err = errors.Join(err, m.ReleaseView(path))
	}
	return err
}

func (m *Manager) trackView(path string, closer ViewCloser) {
	m.mu.Lock()
	m.activeViews[path] = closer
	m.mu.Unlock()
}

func (m *Manager) loadCoreArtifacts(ctx context.Context, mf *protocol.SnapshotManifest) ([]byte, []byte, error) {
	readArt := func(name string) ([]byte, error) {
		localP := filepath.Join(m.cfg.L2CASDir, "snapshots", mf.SnapshotID, name)
		if b, err := os.ReadFile(localP); err == nil {
			return b, nil
		}
		if m.cfg.RemoteStore != nil {
			b, err := m.cfg.RemoteStore.GetBlob(ctx, storage.ArtifactKey(mf.SnapshotID, name))
			if err == nil {
				_ = writeAtomicFile(localP, b)
				return b, nil
			}
			return nil, err
		}
		return nil, fmt.Errorf("artifact %s missing for snapshot %s", name, mf.SnapshotID)
	}
	ckptBytes, err := readArt(protocol.GVisorCheckpointImg)
	if err != nil && mf.CheckpointImgSize > 0 {
		return nil, nil, err
	}
	metaBytes, err := readArt(protocol.GVisorPagesMetaImg)
	if err != nil && mf.PagesMetaImgSize > 0 {
		return nil, nil, err
	}
	return ckptBytes, metaBytes, nil
}

// chunkAndDiffGo is the pure-Go 2MiB Blake3 fallback matching crates/kontinue-arena byte-for-byte.
func (m *Manager) chunkAndDiffGo(opts IngestOptions, parent *protocol.SnapshotManifest) (*protocol.SnapshotManifest, error) {
	parentSet := make(map[string]bool)
	if parent != nil {
		for _, ch := range parent.Chunks {
			if ch.Blake3Hash != "" {
				parentSet[ch.Blake3Hash] = true
			}
		}
	}

	stageSmall := func(name string) (string, int64, error) {
		src := filepath.Join(opts.RawCheckpointDir, name)
		b, err := os.ReadFile(src)
		if os.IsNotExist(err) {
			return "", 0, nil
		}
		if err != nil {
			return "", 0, err
		}
		sum := blake3.Sum256(b)
		h := hex.EncodeToString(sum[:])
		dst := filepath.Join(m.cfg.L2CASDir, "snapshots", opts.SnapshotID, name)
		if err := writeAtomicFile(dst, b); err != nil {
			return "", 0, err
		}
		return h, int64(len(b)), nil
	}

	ckptH, ckptSz, err := stageSmall(protocol.GVisorCheckpointImg)
	if err != nil {
		return nil, err
	}
	metaH, metaSz, err := stageSmall(protocol.GVisorPagesMetaImg)
	if err != nil {
		return nil, err
	}
	rootfsH, rootfsSz, err := stageSmall(protocol.RootfsUpperTar)
	if err != nil {
		return nil, err
	}

	pagesPath := filepath.Join(opts.RawCheckpointDir, protocol.GVisorPagesImg)
	var chunks []protocol.ChunkRef
	var totalSz, dirtyBytes int64
	var dirtyCnt, reusedCnt, zeroCnt int32
	includeMem := false

	if f, openErr := os.Open(pagesPath); openErr == nil {
		includeMem = true
		defer f.Close()
		buf := make([]byte, protocol.DefaultChunkSize)
		var idx uint32
		for {
			n, rErr := io.ReadFull(f, buf)
			if rErr == io.EOF {
				break
			}
			if rErr != nil && rErr != io.ErrUnexpectedEOF {
				return nil, rErr
			}
			slice := buf[:n]
			off := totalSz
			totalSz += int64(n)

			allZero := true
			for _, b := range slice {
				if b != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				zeroCnt++
				chunks = append(chunks, protocol.ChunkRef{
					Index:     idx,
					Offset:    off,
					Length:    int32(n),
					ZeroChunk: true,
				})
			} else {
				sum := blake3.Sum256(slice)
				h := hex.EncodeToString(sum[:])
				reused := parentSet[h]
				chunkFile := filepath.Join(m.cfg.L2CASDir, storage.ChunkKey(h))
				if !reused {
					dirtyCnt++
					dirtyBytes += int64(n)
					if err := writeAtomicFile(chunkFile, slice); err != nil {
						return nil, err
					}
				} else {
					reusedCnt++
					if _, statErr := os.Stat(chunkFile); statErr != nil {
						if err := writeAtomicFile(chunkFile, slice); err != nil {
							return nil, err
						}
					}
				}
				chunks = append(chunks, protocol.ChunkRef{
					Index:            idx,
					Offset:           off,
					Length:           int32(n),
					Blake3Hash:       h,
					ReusedFromParent: reused,
				})
			}
			idx++
			if rErr == io.ErrUnexpectedEOF {
				break
			}
		}
	}

	mf := &protocol.SnapshotManifest{
		SchemaVersion:      1,
		SnapshotID:         opts.SnapshotID,
		Kind:               opts.Kind,
		TemplateID:         opts.TemplateID,
		BaseImage:          opts.BaseImage,
		ParentSnapshotID:   opts.ParentSnapshotID,
		CreatedAt:          time.Now().UTC(),
		IncludeMemory:      includeMem,
		ChunkSize:          protocol.DefaultChunkSize,
		CheckpointImgHash:  ckptH,
		CheckpointImgSize:  ckptSz,
		PagesMetaImgHash:   metaH,
		PagesMetaImgSize:   metaSz,
		PagesImgTotalSize:  totalSz,
		DirtyMemoryBytes:   dirtyBytes,
		DirtyChunkCount:    dirtyCnt,
		ReusedChunkCount:   reusedCnt,
		ZeroChunkCount:     zeroCnt,
		RootfsUpperTarHash: rootfsH,
		RootfsUpperTarSize: rootfsSz,
		Chunks:             chunks,
		OriginNodeID:       m.cfg.NodeID,
		OriginPeerAddr:     m.cfg.P2PAddr,
		BootstrapToken:     opts.BootstrapToken,
		Fingerprint:        opts.Fingerprint,
	}
	return mf, nil
}

func cloneDirHardlinks(srcDir, dstDir string) error {
	for _, name := range []string{protocol.GVisorCheckpointImg, protocol.GVisorPagesMetaImg, protocol.GVisorPagesImg} {
		if err := os.Link(filepath.Join(srcDir, name), filepath.Join(dstDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// WriteCheckpointDescriptor supplies Kubelet's requested output on a replayed CRI call.
func (m *Manager) WriteCheckpointDescriptor(path string, mf *protocol.SnapshotManifest, uri string) error {
	localManifest := filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(mf.SnapshotID, protocol.ManifestFileName))
	return writeKubeletCheckpointTar(path, mf, uri, localManifest)
}

func writeKubeletCheckpointTar(tarPath string, mf *protocol.SnapshotManifest, manifestURI, localArenaPath string) error {
	if err := os.MkdirAll(filepath.Dir(tarPath), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(tarPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	defer tw.Close()

	desc := protocol.KubeletCheckpointDescriptor{
		SnapshotID:     mf.SnapshotID,
		ArenaLocalPath: localArenaPath,
		ManifestURI:    manifestURI,
		CreatedAt:      mf.CreatedAt,
		TotalBytes:     mf.PagesImgTotalSize + mf.CheckpointImgSize + mf.PagesMetaImgSize,
		DirtyBytes:     mf.DirtyMemoryBytes,
	}
	descRaw, _ := json.MarshalIndent(desc, "", "  ")
	mfRaw, _ := json.MarshalIndent(mf, "", "  ")

	addEntry := func(name string, body []byte) error {
		if err := tw.WriteHeader(&tar.Header{
			Name:    name,
			Mode:    0o600,
			Size:    int64(len(body)),
			ModTime: time.Now(),
		}); err != nil {
			return err
		}
		_, err := tw.Write(body)
		return err
	}
	if err := addEntry("kontinue-descriptor.json", descRaw); err != nil {
		return err
	}
	return addEntry(protocol.ManifestFileName, mfRaw)
}
