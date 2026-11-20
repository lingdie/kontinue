package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/apiserver"
	"github.com/kontinue-sh/kontinue/internal/crishim"
	"github.com/kontinue-sh/kontinue/internal/gateway"
	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/internal/node/hydrator"
	"github.com/kontinue-sh/kontinue/internal/node/p2p"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// realRustKtdDriver launches actual compiled Rust `bin/ktd` child processes for each sandbox container
// and persists/restores simulated gVisor split memory images + container rootfs overlays across checkpoints!
type realRustKtdDriver struct {
	ktdBin string
	root   string

	mu    sync.Mutex
	procs map[string]*liveKtdInstance
	seq   atomic.Int64
}

type liveKtdInstance struct {
	cmd       *exec.Cmd
	workDir   string
	memRegion []byte
	addr      string
}

func newRealRustKtdDriver(t *testing.T, ktdBin string) *realRustKtdDriver {
	return &realRustKtdDriver{
		ktdBin: ktdBin,
		root:   t.TempDir(),
		procs:  make(map[string]*liveKtdInstance),
	}
}

func (d *realRustKtdDriver) ShutdownAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, p := range d.procs {
		if p.cmd != nil && p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
			_, _ = p.cmd.Process.Wait()
		}
	}
}

func (d *realRustKtdDriver) SwapAndStartSandboxContainer(ctx context.Context, opts hydrator.SwapContainerOptions) (string, string, error) {
	idNum := d.seq.Add(1)
	cid := fmt.Sprintf("kt-gvisor-ctr-%d", idNum)
	workDir := filepath.Join(d.root, cid, "rootfs")
	cgroupDir := filepath.Join(d.root, cid, "cgroup")
	_ = os.MkdirAll(workDir, 0o755)
	_ = os.MkdirAll(cgroupDir, 0o755)
	_ = os.WriteFile(filepath.Join(cgroupDir, "cgroup.freeze"), []byte("0\n"), 0o644)

	// Base 6 MiB simulated memory (Chunk 0 = 'T' template code, Chunk 1 = Zero, Chunk 2 = 'D' heap)
	mem := make([]byte, 3*protocol.DefaultChunkSize)
	for i := 0; i < protocol.DefaultChunkSize; i++ {
		mem[i] = 'T'
		mem[2*protocol.DefaultChunkSize+i] = 'D'
	}

	var inherited protocol.HealthResponse
	if opts.RestoreViewDir != "" && opts.RestoreMode == protocol.RestoreModeMemory {
		meta, err := os.ReadFile(filepath.Join(opts.RestoreViewDir, protocol.GVisorPagesMetaImg))
		if err != nil {
			return "", "", err
		}
		if err := json.Unmarshal(meta, &inherited); err != nil {
			return "", "", err
		}
		if restoredMem, err := os.ReadFile(filepath.Join(opts.RestoreViewDir, protocol.GVisorPagesImg)); err == nil {
			mem = restoredMem
		}
		// Restore workspace state saved inside checkpoint.img payload
		if savedState, err := os.ReadFile(filepath.Join(opts.RestoreViewDir, protocol.GVisorCheckpointImg)); err == nil {
			var files map[string][]byte
			if json.Unmarshal(savedState, &files) == nil {
				for rel, content := range files {
					p := filepath.Join(workDir, rel)
					_ = os.MkdirAll(filepath.Dir(p), 0o755)
					_ = os.WriteFile(p, content, 0o644)
				}
			}
		}
	}

	// Pick free localhost port for this sandbox's Rust ktd instance
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", err
	}
	addr := l.Addr().String()
	_ = l.Close()

	cmd := exec.Command(d.ktdBin,
		"--listen", addr,
		"--bootstrap-token", opts.BootstrapToken,
		"--initial-access-token", opts.AccessToken,
		"--sandbox-id", opts.SandboxID,
		"--machine-id-path", filepath.Join(workDir, "etc", "machine-id"),
		"--workload-cgroup", cgroupDir,
	)
	cmd.Dir = workDir
	if err := cmd.Start(); err != nil {
		return "", "", err
	}

	d.mu.Lock()
	d.procs[cid] = &liveKtdInstance{
		cmd:       cmd,
		workDir:   workDir,
		memRegion: mem,
		addr:      addr,
	}
	d.mu.Unlock()
	// A real memory restore resumes the original ktd, including its monotonic fence.
	if inherited.ExecutionID > 0 {
		startup, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		client := protocol.NewKtdClient(time.Second)
		for {
			_, err := client.HeldInit(startup, addr, opts.BootstrapToken, protocol.InitPayload{
				SandboxID: inherited.SandboxID, ExecutionID: inherited.ExecutionID, SkipThaw: true,
			})
			if err == nil {
				break
			}
			if startup.Err() != nil {
				return "", "", fmt.Errorf("restore test guest identity: %w", err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return cid, addr, nil
}

func (d *realRustKtdDriver) CheckpointSandboxContainer(ctx context.Context, containerID, rawOutputDir string, leaveRunning bool) error {
	d.mu.Lock()
	inst, ok := d.procs[containerID]
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("container %s not running", containerID)
	}
	if err := os.MkdirAll(rawOutputDir, 0o700); err != nil {
		return err
	}

	// Capture workspace files into checkpoint.img and mutate dirty heap chunk if state changed
	files := make(map[string][]byte)
	_ = filepath.Walk(inst.workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(inst.workDir, path)
		if b, rErr := os.ReadFile(path); rErr == nil {
			files[rel] = b
		}
		return nil
	})
	ckptState, _ := json.Marshal(files)

	pages := append([]byte(nil), inst.memRegion...)
	if len(files) > 1 {
		// Simulate dirty heap mutation in Chunk 2 only (Chunk 0 stays identical to Golden Template, Chunk 1 stays Zero!)
		for i := 0; i < 4096; i++ {
			pages[2*protocol.DefaultChunkSize+i] = byte('M' + (len(files) % 7))
		}
	}

	if err := os.WriteFile(filepath.Join(rawOutputDir, protocol.GVisorCheckpointImg), ckptState, 0o600); err != nil {
		return err
	}
	health, err := protocol.NewKtdClient(time.Second).Health(ctx, inst.addr, "")
	if err != nil {
		return err
	}
	guestIdentity, err := json.Marshal(health)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(rawOutputDir, protocol.GVisorPagesMetaImg), guestIdentity, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(rawOutputDir, protocol.GVisorPagesImg), pages, 0o600); err != nil {
		return err
	}

	if !leaveRunning && inst.cmd != nil && inst.cmd.Process != nil {
		_ = inst.cmd.Process.Kill()
		_, _ = inst.cmd.Process.Wait()
	}
	return nil
}

func (d *realRustKtdDriver) StopSandboxContainer(_ context.Context, _ protocol.CarrierSlot, activeContainerID string) error {
	d.mu.Lock()
	inst, ok := d.procs[activeContainerID]
	if ok {
		delete(d.procs, activeContainerID)
	}
	d.mu.Unlock()
	if ok && inst.cmd != nil && inst.cmd.Process != nil {
		_ = inst.cmd.Process.Kill()
		_, _ = inst.cmd.Process.Wait()
	}
	return nil
}

// mockKubeletBridge routes Kubelet POST /checkpoint/{ns}/{pod}/{ctr} directly through kontinue-cri-shim!
type mockKubeletBridge struct {
	t       *testing.T
	shims   map[string]*crishim.Proxy
	ckptDir string
}

func (k *mockKubeletBridge) CheckpointContainer(ctx context.Context, nodeIP string, _ int, ns, pod, ctr string, timeout int) ([]string, error) {
	shim, ok := k.shims[nodeIP]
	if !ok {
		return nil, fmt.Errorf("unknown nodeIP %s", nodeIP)
	}
	tarPath := filepath.Join(k.ckptDir, fmt.Sprintf("checkpoint-%s-%s-%d.tar", pod, ctr, time.Now().UnixNano()))
	_, err := shim.CheckpointContainer(ctx, &runtimeapi.CheckpointContainerRequest{
		ContainerId: "placeholder-" + pod,
		Location:    tarPath,
		Timeout:     int64(timeout),
	})
	if err != nil {
		return nil, err
	}
	return []string{tarPath}, nil
}

// Interrupt the metadata commit after the node has already produced a snapshot.
type interruptedSuspend struct {
	store.MetadataStore
	catalog     store.RuntimeCatalog
	interrupted atomic.Bool
}

func (m *interruptedSuspend) SaveSandbox(ctx context.Context, sbx *store.SandboxRecord) error {
	if sbx.State == store.SandboxStateSuspended && m.interrupted.CompareAndSwap(false, true) {
		_, err := m.catalog.AdmitRoute(ctx, sbx.SandboxID, "after-capture")
		if !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("unexpected admission during suspend: %v", err)
		}
		health, err := protocol.NewKtdClient(time.Second).Health(ctx, sbx.PodIP, sbx.AccessToken)
		if err != nil {
			return err
		}
		if !health.Frozen {
			return errors.New("guest thawed before suspend commit")
		}
		return errors.New("injected suspend commit failure")
	}
	return m.MetadataStore.SaveSandbox(ctx, sbx)
}

func TestFullPolyglotPipeline_GoldenTemplate_Clone_Fork_Suspend_CrossNodeP2PResume_And_Traces(t *testing.T) {
	ctx := context.Background()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repoRoot: %v", err)
	}
	ktdBin := filepath.Join(repoRoot, "bin", "ktd")
	arenaHelperBin := filepath.Join(repoRoot, "bin", "kontinue-arena-helper")
	for _, b := range []string{ktdBin, arenaHelperBin} {
		if _, err := os.Stat(b); err != nil {
			t.Fatalf("required Rust binary %s not built (run `make build-rust` first): %v", b, err)
		}
	}

	prov, err := telemetry.Setup(ctx, telemetry.Config{ServiceName: "kontinue-e2e", RingBufferSize: 4096})
	if err != nil {
		t.Fatalf("telemetry.Setup: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	var catalog store.RuntimeCatalog = store.NewMemoryRuntimeCatalog()
	if addr := os.Getenv("KONTINUE_TEST_REDIS"); addr != "" {
		redis := store.NewRedisRuntimeCatalog(addr, "", 0)
		defer redis.Close()
		catalog = redis
	}
	var db store.MetadataStore = store.NewMemoryMetadataStore()
	if dsn := os.Getenv("KONTINUE_TEST_POSTGRES"); dsn != "" {
		pg, err := store.NewPostgresMetadataStore(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close()
		if err := pg.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		db = pg
	}
	meta := &interruptedSuspend{MetadataStore: db, catalog: catalog}
	sharedCAS, _ := storage.NewLocalFSBackend(t.TempDir())
	sharedFP := protocol.CompatibilityFingerprint{
		Architecture:   "arm64",
		CPUProfileHash: "apple-m-systrap",
		RunscVersion:   "runsc-20261005",
		Arm64NoPauth:   true,
	}

	driver := newRealRustKtdDriver(t, ktdBin)
	defer driver.ShutdownAll()

	shimsByIP := make(map[string]*crishim.Proxy)

	startWorkerNode := func(nodeID, nodeIP string) (grpcAddr string, debugURL string, cleanup func()) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", nodeID, err)
		}
		arenaMgr, err := arena.NewManager(arena.ManagerConfig{
			NodeID:          nodeID,
			P2PAddr:         lis.Addr().String(),
			L1TmpfsDir:      t.TempDir(),
			L2CASDir:        t.TempDir(),
			ViewsDir:        t.TempDir(),
			RustArenaHelper: arenaHelperBin,
			RemoteStore:     sharedCAS,
			Recorder:        prov.Recorder(),
		})
		if err != nil {
			t.Fatalf("NewManager: %v", err)
		}

		mapper := crishim.NewMapper()
		shimAdmin := httptest.NewServer(mapper.HTTPHandler())

		hydSvc := hydrator.NewService(hydrator.ServiceConfig{
			NodeID:       nodeID,
			NodeIP:       nodeIP,
			NodeGRPCAddr: lis.Addr().String(),
			ScratchDir:   t.TempDir(),
			ShimAdminURL: shimAdmin.URL,
			Fingerprint:  sharedFP,
			Arena:        arenaMgr,
			Driver:       driver,
			Routes:       catalog,
		})

		grpcSrv := grpc.NewServer(
			grpc.UnaryInterceptor(telemetry.UnaryServerInterceptor(nodeID)),
			grpc.StreamInterceptor(telemetry.StreamServerInterceptor(nodeID)),
		)
		protocol.RegisterNodeControlServer(grpcSrv, hydSvc)
		protocol.RegisterNodeArenaP2PServer(grpcSrv, p2p.NewServer(arenaMgr))
		go func() { _ = grpcSrv.Serve(lis) }()

		cc, _ := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(telemetry.UnaryClientInterceptor("cri-shim-"+nodeID)),
		)
		shimsByIP[nodeIP] = crishim.NewProxyFromClients(nil, nil, protocol.NewNodeControlClient(cc), mapper)

		debugSrv := httptest.NewServer(http.HandlerFunc(prov.Recorder().Handler()))

		_ = catalog.UpsertNodeHeartbeat(ctx, protocol.NodeHeartbeat{
			NodeID:        nodeID,
			NodeIP:        nodeIP,
			GRPCAddr:      lis.Addr().String(),
			P2PAddr:       lis.Addr().String(),
			KubeletPort:   10250,
			TotalMemBytes: 16 << 30,
			FreeMemBytes:  12 << 30,
			Fingerprint:   sharedFP,
		}, 5*time.Minute)

		return lis.Addr().String(), debugSrv.URL, func() {
			debugSrv.Close()
			shimAdmin.Close()
			_ = cc.Close()
			grpcSrv.Stop()
			_ = arenaMgr.Close()
		}
	}

	nodeAGRPC, nodeADebugURL, cleanA := startWorkerNode("worker-node-a", "10.0.0.11")
	defer cleanA()
	nodeBGRPC, _, cleanB := startWorkerNode("worker-node-b", "10.0.0.12")
	defer cleanB()

	// Seed pre-warmed Carrier Pod slots on Node A (for build, clone, fork)
	for i := 1; i <= 4; i++ {
		podName := fmt.Sprintf("carrier-a-%d", i)
		_ = catalog.RegisterIdleSlot(ctx, protocol.CarrierSlot{
			SlotID:                fmt.Sprintf("slot-a-%d", i),
			NodeID:                "worker-node-a",
			NodeIP:                "10.0.0.11",
			NodeGRPCAddr:          nodeAGRPC,
			Tier:                  protocol.DefaultCarrierTier,
			PodNamespace:          "kontinue-sandboxes",
			PodName:               podName,
			PodUID:                fmt.Sprintf("uid-a-%d", i),
			PodIP:                 "127.0.0.1",
			PodSandboxID:          fmt.Sprintf("podsbx-a-%d", i),
			PlaceholderID:         "placeholder-" + podName,
			ExpectedContainerHash: "pleg-hash-ok",
		})
	}

	kubeletMock := &mockKubeletBridge{t: t, shims: shimsByIP, ckptDir: t.TempDir()}
	apiSrvImpl := apiserver.NewServer(apiserver.ServerConfig{
		Meta:     meta,
		Catalog:  catalog,
		Kubelet:  kubeletMock,
		Recorder: prov.Recorder(),
	})
	defer apiSrvImpl.Close()
	apiHTTP := httptest.NewServer(apiSrvImpl.Handler())
	defer apiHTTP.Close()

	gwImpl := gateway.NewGateway(gateway.Config{
		Catalog:      catalog,
		APIServerURL: apiHTTP.URL,
		BaseDomain:   "sandbox.local",
		Recorder:     prov.Recorder(),
	})
	gwHTTP := httptest.NewServer(gwImpl.Handler())
	defer gwHTTP.Close()

	httpClient := &http.Client{Timeout: 20 * time.Second}

	// =========================================================================
	// 1. Build Golden Memory Template (Warmup -> Kubelet /checkpoint -> L1 Pin)
	// =========================================================================
	tplReqBody, _ := json.Marshal(apiserver.CreateTemplateRequest{
		Name:          "python-agent-v1",
		BaseImage:     "kontinue/python-agent:3.12",
		WarmupScript:  "printf 'baked-in-golden-memory' > ./golden_marker.txt",
		PinInL1Tmpfs:  true,
		TraceHotPages: true,
	})
	tplResp, err := httpClient.Post(apiHTTP.URL+"/v1/templates", "application/json", bytes.NewReader(tplReqBody))
	if err != nil || tplResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(tplResp.Body)
		t.Fatalf("POST /v1/templates failed (status=%v, err=%v): %s", tplResp.StatusCode, err, string(body))
	}
	var createdTpl store.TemplateRecord
	_ = json.NewDecoder(tplResp.Body).Decode(&createdTpl)
	_ = tplResp.Body.Close()
	if createdTpl.Phase != "Ready" || createdTpl.GoldenSnapshotID == "" {
		t.Fatalf("expected Ready template with GoldenSnapshotID, got %+v", createdTpl)
	}

	// =========================================================================
	// 2. 1-to-N Fast Clone + Idempotency + Dynamic Secret Env Injection
	// =========================================================================
	sbxReqBody, _ := json.Marshal(apiserver.CreateSandboxRequest{
		SandboxID:  "sbx-agent-007",
		TemplateID: createdTpl.TemplateID,
		EnvVars: map[string]string{
			"OPENAI_API_KEY": "sk-tenant-secret-never-in-template",
		},
		IdleTimeoutSeconds: 30,
	})
	createReq, _ := http.NewRequest(http.MethodPost, apiHTTP.URL+"/v1/sandboxes", bytes.NewReader(sbxReqBody))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set(protocol.HeaderIdempotencyKey, "idem-create-007")
	cResp, err := httpClient.Do(createReq)
	if err != nil || cResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateSandbox failed: %v status=%d", err, cResp.StatusCode)
	}
	var sbx store.SandboxRecord
	_ = json.NewDecoder(cResp.Body).Decode(&sbx)
	_ = cResp.Body.Close()

	if sbx.AccessToken == "" || sbx.BootstrapToken != "" || len(sbx.EnvVars) > 0 {
		t.Fatal("unexpected credentials in create response")
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		path := apiHTTP.URL + "/v1/sandboxes/" + sbx.SandboxID
		if method == http.MethodPost {
			path += "/resume"
		}
		check, _ := http.NewRequest(method, path, nil)
		result, err := httpClient.Do(check)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]json.RawMessage
		err = json.NewDecoder(result.Body).Decode(&payload)
		result.Body.Close()
		if err != nil || result.StatusCode != http.StatusOK || payload["sandboxId"] == nil {
			t.Fatalf("metadata response: %v, %d", err, result.StatusCode)
		}
		for _, key := range []string{"envVars", "accessToken", "bootstrapToken", "pendingPlacement", "pendingFork"} {
			if _, exists := payload[key]; exists {
				t.Fatalf("metadata response contains %s", key)
			}
		}
	}
	if _, err := apiSrvImpl.CreateSandbox(ctx, apiserver.CreateSandboxRequest{SandboxID: sbx.SandboxID}); !errors.Is(err, store.ErrSandboxExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	route, err := catalog.GetRoute(ctx, sbx.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	conflicting := *route
	conflicting.SlotID = "different-slot"
	if err := catalog.PublishRoute(ctx, conflicting); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatalf("conflicting slot in same generation: %v", err)
	}
	if err := catalog.PublishRoute(ctx, *route); err != nil {
		t.Fatalf("idempotent route publication: %v", err)
	}
	// Security check: Gateway MUST reject external calls to /internal/init!
	secReq, _ := http.NewRequest(http.MethodPost, gwHTTP.URL+protocol.PathInternalInit, strings.NewReader(`{}`))
	secReq.Header.Set(protocol.HeaderKontinueSandboxID, sbx.SandboxID)
	secResp, _ := httpClient.Do(secReq)
	if secResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected gateway to block /internal/init with 404, got %d", secResp.StatusCode)
	}
	_ = secResp.Body.Close()

	// Verify Golden Template state + dynamically injected secret env via Gateway -> Rust ktd!
	execThroughGateway := func(sandboxID, token, shellCmd string) (*protocol.ExecResponse, string, error) {
		raw, _ := json.Marshal(protocol.ExecRequest{Command: []string{"/bin/sh", "-c", shellCmd}})
		req, _ := http.NewRequest(http.MethodPost, gwHTTP.URL+protocol.PathProcessExec, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(protocol.HeaderKontinueSandboxID, sandboxID)
		req.Header.Set(protocol.HeaderKontinueToken, token)
		resp, doErr := httpClient.Do(req)
		if doErr != nil {
			return nil, "", doErr
		}
		defer resp.Body.Close()
		traceID := resp.Header.Get(protocol.HeaderTraceID)
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return nil, traceID, fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
		}
		var out protocol.ExecResponse
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return &out, traceID, nil
	}

	out1, _, err := execThroughGateway(sbx.SandboxID, sbx.AccessToken,
		"printf '%s|%s' \"$(cat ./golden_marker.txt)\" \"$OPENAI_API_KEY\" && printf 'turn-1-state' > ./session.txt")
	if err != nil {
		t.Fatalf("exec on cloned sandbox: %v", err)
	}
	if out1.Stdout != "baked-in-golden-memory|sk-tenant-secret-never-in-template" {
		t.Fatalf("unexpected clone stdout: %q", out1.Stdout)
	}

	// =========================================================================
	// 3. 1-to-K Live Stateful Fork (Tree-of-Thought Branching)
	// =========================================================================
	forkBody, _ := json.Marshal(apiserver.ForkSandboxRequest{Count: 2})
	fResp, err := httpClient.Post(fmt.Sprintf("%s/v1/sandboxes/%s/fork", apiHTTP.URL, sbx.SandboxID), "application/json", bytes.NewReader(forkBody))
	if err != nil || fResp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(fResp.Body)
		t.Fatalf("ForkSandbox failed (%v): %s", err, string(b))
	}
	var forks []*store.SandboxRecord
	_ = json.NewDecoder(fResp.Body).Decode(&forks)
	_ = fResp.Body.Close()
	if len(forks) != 2 {
		t.Fatalf("expected 2 forked sandboxes, got %d", len(forks))
	}
	forkOut, _, err := execThroughGateway(forks[0].SandboxID, forks[0].AccessToken, "cat ./session.txt")
	if err != nil || forkOut.Stdout != "turn-1-state" {
		t.Fatalf("forked sandbox failed to inherit live session state: out=%+v err=%v", forkOut, err)
	}

	// =========================================================================
	// 4. 1-to-1 Suspend (Blake3 Diff) + Cross-Node P2P Singleflight Auto-Resume
	// =========================================================================
	if _, err := catalog.AdmitRoute(ctx, sbx.SandboxID, "long-request"); err != nil {
		t.Fatal(err)
	}
	if _, err := apiSrvImpl.SuspendSandbox(ctx, sbx.SandboxID); !errors.Is(err, store.ErrTransitionLocked) {
		t.Fatalf("suspend with active request: %v", err)
	}
	if err := catalog.FinishAdmission(ctx, sbx.SandboxID, "long-request"); err != nil {
		t.Fatal(err)
	}
	if _, err := apiSrvImpl.SuspendSandbox(ctx, sbx.SandboxID); err == nil || err.Error() != "injected suspend commit failure" {
		t.Fatalf("interrupted suspend: %v", err)
	}
	intent, err := meta.GetSandbox(ctx, sbx.SandboxID)
	if err != nil || intent.State != store.SandboxStateSuspending {
		t.Fatalf("missing suspend intent: %v", err)
	}
	recovered := apiserver.NewServer(apiserver.ServerConfig{Meta: meta, Catalog: catalog, Kubelet: kubeletMock})
	recovered.RunEvictionAndGCSweep(ctx)
	recovered.Close()
	finished, err := meta.GetSandbox(ctx, sbx.SandboxID)
	if err != nil || finished.State != store.SandboxStateSuspended || finished.CurrentSlotID != "" || finished.ActiveSnapshotID != intent.PendingSnapshotID {
		t.Fatalf("sweep did not finish interrupted suspend: err=%v", err)
	}
	suspResp, err := httpClient.Post(fmt.Sprintf("%s/v1/sandboxes/%s/suspend", apiHTTP.URL, sbx.SandboxID), "application/json", nil)
	if err != nil || suspResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(suspResp.Body)
		t.Fatalf("SuspendSandbox failed (%v): %s", err, string(b))
	}
	var suspSnap store.SnapshotRecord
	_ = json.NewDecoder(suspResp.Body).Decode(&suspSnap)
	_ = suspResp.Body.Close()

	// Verify 2MiB Blake3 incremental deduplication reused unchanged template chunk!
	if suspSnap.BootstrapToken != "" {
		t.Fatal("suspend disclosed the bootstrap credential")
	}
	if suspSnap.DirtyChunks != 1 || suspSnap.ReusedChunks != 1 {
		t.Fatalf("expected incremental Blake3 dedup (1 dirty, 1 reused), got dirty=%d reused=%d", suspSnap.DirtyChunks, suspSnap.ReusedChunks)
	}

	// Drain remaining slots on Node A and register a slot ONLY on Node B to force Cross-Node P2P + Lazy Arena restore!
	for {
		if _, cErr := catalog.ClaimIdleSlot(ctx, "worker-node-a", protocol.DefaultCarrierTier); cErr != nil {
			break
		}
	}
	slotB := protocol.CarrierSlot{
		SlotID:                "slot-b-1",
		NodeID:                "worker-node-b",
		NodeIP:                "10.0.0.12",
		NodeGRPCAddr:          nodeBGRPC,
		Tier:                  protocol.DefaultCarrierTier,
		PodNamespace:          "kontinue-sandboxes",
		PodName:               "carrier-b-1",
		PodUID:                "uid-b-1",
		PodIP:                 "127.0.0.1",
		PodSandboxID:          "podsbx-b-1",
		PlaceholderID:         "placeholder-carrier-b-1",
		ExpectedContainerHash: "pleg-hash-b",
	}
	if err := catalog.RegisterIdleSlot(ctx, slotB); err != nil {
		t.Fatal(err)
	}

	// Fire 16 concurrent Agent requests at kontinue-gateway while sbx-agent-007 is SUSPENDED!
	const concurrentCallers = 16
	var wg sync.WaitGroup
	errs := make(chan error, concurrentCallers)
	traceIDs := make(chan string, concurrentCallers)
	for c := 0; c < concurrentCallers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, tid, eErr := execThroughGateway(sbx.SandboxID, sbx.AccessToken, "cat ./session.txt")
			if eErr != nil {
				errs <- eErr
				return
			}
			if res.Stdout != "turn-1-state" {
				errs <- fmt.Errorf("unexpected stdout after cross-node auto-resume: %q", res.Stdout)
				return
			}
			traceIDs <- tid
		}()
	}
	wg.Wait()
	close(errs)
	close(traceIDs)
	for e := range errs {
		t.Fatalf("concurrent auto-resume caller failed: %v", e)
	}

	resumedSbx, err := meta.GetSandbox(ctx, sbx.SandboxID)
	if err != nil {
		t.Fatalf("GetSandbox: %v", err)
	}
	if resumedSbx.ExecutionID != 2 || resumedSbx.CurrentNodeID != "worker-node-b" {
		t.Fatalf("expected singleflight resume to executionId=2 on worker-node-b, got exec=%d node=%s",
			resumedSbx.ExecutionID, resumedSbx.CurrentNodeID)
	}

	slotB.SlotID = "slot-b-2"
	slotB.PodName = "carrier-b-2"
	slotB.PodUID = "uid-b-2"
	slotB.PodSandboxID = "podsbx-b-2"
	slotB.PlaceholderID = "placeholder-carrier-b-2"
	if err := catalog.RegisterIdleSlot(ctx, slotB); err != nil {
		t.Fatal(err)
	}
	forkReq := apiserver.ForkSandboxRequest{Count: 1, RequestKey: "resume-and-fork"}
	branches, err := apiSrvImpl.ForkSandbox(ctx, sbx.SandboxID, forkReq)
	if err != nil {
		t.Fatalf("fork resumed sandbox: %v", err)
	}
	branch := branches[0]
	replay, err := apiSrvImpl.ForkSandbox(ctx, sbx.SandboxID, forkReq)
	if err != nil || len(replay) != 1 || replay[0].SandboxID != branch.SandboxID || replay[0].LifecycleID != branch.LifecycleID || replay[0].AccessToken != branch.AccessToken {
		t.Fatalf("lost fork response failed to replay: %v", err)
	}
	forkReq.Count++
	if _, err := apiSrvImpl.ForkSandbox(ctx, sbx.SandboxID, forkReq); !errors.Is(err, store.ErrTransitionLocked) {
		t.Fatalf("reused fork key accepted changed request: %v", err)
	}
	health, err := protocol.NewKtdClient(time.Second).Health(ctx, branch.PodIP, branch.AccessToken)
	if err != nil || health.SandboxID != branch.SandboxID || health.ExecutionID != branch.ExecutionID || branch.ExecutionID <= resumedSbx.ExecutionID {
		t.Fatalf("fork guest identity: %+v err=%v", health, err)
	}
	if out, _, err := execThroughGateway(branch.SandboxID, branch.AccessToken, "cat ./session.txt"); err != nil || out.Stdout != "turn-1-state" {
		t.Fatalf("resumed fork command: %+v err=%v", out, err)
	}

	// =========================================================================
	// 5. Verify Unified Cross-Language (Go + Rust ktd) Trace Waterfall!
	// =========================================================================
	var sampleTraceID string
	for tid := range traceIDs {
		if tid != "" {
			sampleTraceID = tid
			break
		}
	}
	if sampleTraceID == "" {
		t.Fatalf("expected non-empty X-Trace-ID header from gateway")
	}

	// Query both Go FlightRecorder AND live Rust ktd's /debug/traces endpoint!
	liveRoute, err := catalog.GetRoute(ctx, sbx.SandboxID)
	if err != nil {
		t.Fatalf("GetRoute after resume: %v", err)
	}
	var allSpans []telemetry.RecordedSpan
	for _, ep := range []string{nodeADebugURL, "http://" + liveRoute.PodIP} {
		u := ep + protocol.PathDebugTraces + "?sandbox_id=" + url.QueryEscape(sbx.SandboxID)
		resp, gErr := httpClient.Get(u)
		if gErr != nil {
			t.Fatalf("GET %s: %v", u, gErr)
		}
		var dr telemetry.DebugTraceResponse
		_ = json.NewDecoder(resp.Body).Decode(&dr)
		_ = resp.Body.Close()
		allSpans = append(allSpans, dr.Spans...)
	}

	var servicesSeen []string
	for _, sp := range allSpans {
		if !slices.Contains(servicesSeen, sp.Service) {
			servicesSeen = append(servicesSeen, sp.Service)
		}
	}
	// Both Go ("kontinue-e2e") and Rust ("ktd") must appear in the unified trace query!
	if !slices.Contains(servicesSeen, "kontinue-e2e") || !slices.Contains(servicesSeen, "ktd") {
		t.Fatalf("expected spans from both Go services and Rust ktd, got services=%v", servicesSeen)
	}
}
