package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/internal/node/hydrator"
	"github.com/kontinue-sh/kontinue/internal/node/p2p"
	"github.com/kontinue-sh/kontinue/internal/node/probe"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func main() {
	var (
		nodeID        = flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes Node name")
		nodeIP        = flag.String("node-ip", os.Getenv("NODE_IP"), "Node routable IP")
		grpcListen    = flag.String("grpc-listen", ":9090", "NodeControl & P2P gRPC listen address")
		httpListen    = flag.String("http-listen", ":8082", "Health & /debug/traces HTTP listen address")
		criSock       = flag.String("containerd-sock", "/run/containerd/containerd.sock", "Upstream containerd socket")
		shimAdminSock = flag.String("shim-admin-sock", "/run/kontinue/cri-shim-admin.sock", "kontinue-cri-shim admin socket")
		hooksSock     = flag.String("arena-hooks-sock", "/run/kontinue/arena-hooks.sock", "Unix socket for kontinue-runsc callbacks")
		l1Dir         = flag.String("l1-tmpfs-dir", "/run/kontinue-arena/l1-tmpfs", "L1 tmpfs directory for pinned golden templates")
		l2Dir         = flag.String("l2-cas-dir", "/var/lib/kontinue-arena/cas", "L2 NVMe CAS chunk directory")
		viewsDir      = flag.String("views-dir", "/var/lib/kontinue-arena/views", "Restore views & ArenaFS mount root")
		arenaHelper   = flag.String("arena-helper", "/usr/local/bin/kontinue-arena-helper", "Path to Rust kontinue-arena-helper binary")
		runscWrapper  = flag.String("runsc-bin", "/usr/local/bin/kontinue-runsc", "Path to Rust kontinue-runsc wrapper")
		redisAddr     = flag.String("redis-addr", os.Getenv("REDIS_ADDR"), "Redis address (empty uses in-memory catalog)")
		casURL        = flag.String("cas-url", os.Getenv("CAS_HTTP_URL"), "S3/MinIO HTTP base URL (empty uses local FS CAS)")
		localCASRoot  = flag.String("local-cas-fallback", "/var/lib/kontinue-arena/remote-cas", "Fallback shared CAS directory")
	)
	flag.Parse()

	if *nodeID == "" {
		h, _ := os.Hostname()
		*nodeID = h
	}
	if *nodeIP == "" {
		*nodeIP = "127.0.0.1"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	prov, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName: "kontinued",
		NodeID:      *nodeID,
	})
	if err != nil {
		log.Fatalf("telemetry setup: %v", err)
	}
	defer func() { _ = prov.Shutdown(context.Background()) }()

	var casBackend storage.Backend
	if *casURL != "" {
		casBackend = storage.NewHTTPObjectStoreBackend(*casURL, os.Getenv("CAS_AUTH_TOKEN"), nil)
	} else {
		casBackend, err = storage.NewLocalFSBackend(*localCASRoot)
		if err != nil {
			log.Fatalf("init local CAS: %v", err)
		}
	}

	var catalog store.RuntimeCatalog
	if *redisAddr != "" {
		catalog = store.NewRedisRuntimeCatalog(*redisAddr, os.Getenv("REDIS_PASSWORD"), 0)
	} else {
		catalog = store.NewMemoryRuntimeCatalog()
	}

	advertiseGRPC := *nodeIP + *grpcListen
	arenaMgr, err := arena.NewManager(arena.ManagerConfig{
		NodeID:          *nodeID,
		P2PAddr:         advertiseGRPC,
		L1TmpfsDir:      *l1Dir,
		L2CASDir:        *l2Dir,
		ViewsDir:        *viewsDir,
		HooksSockPath:   *hooksSock,
		RustArenaHelper: *arenaHelper,
		RemoteStore:     casBackend,
		Recorder:        prov.Recorder(),
	})
	if err != nil {
		log.Fatalf("arena manager: %v", err)
	}
	defer arenaMgr.Close()

	criConn, err := grpc.NewClient("unix://"+*criSock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial containerd: %v", err)
	}
	defer criConn.Close()

	fp := probe.DetectNodeFingerprint(*runscWrapper)
	criRT := runtimeapi.NewRuntimeServiceClient(criConn)
	driver := hydrator.NewGVisorSandboxDriver(hydrator.GVisorDriverConfig{
		RunscWrapperBin: *runscWrapper,
		RunscStateRoot:  "/run/kontinue/runsc",
		BundlesDir:      "/var/lib/kontinue-arena/bundles",
		BaseRootfsCache: "/var/lib/kontinue-arena/base-rootfs",
	})
	svc := hydrator.NewService(hydrator.ServiceConfig{
		NodeID:       *nodeID,
		NodeIP:       *nodeIP,
		NodeGRPCAddr: advertiseGRPC,
		ScratchDir:   filepath.Join(*l2Dir, "scratch"),
		ShimAdminURL: *shimAdminSock,
		Fingerprint:  fp,
		Arena:        arenaMgr,
		Driver:       driver,
		Routes:       catalog,
	})

	watcher := hydrator.NewCarrierPodWatcher(criRT, catalog, hydrator.WatcherConfig{
		NodeID:       *nodeID,
		NodeIP:       *nodeIP,
		NodeGRPCAddr: advertiseGRPC,
	})
	svc.SetCarrierWatcher(watcher)
	watcher.Start(ctx)

	// Boot-time self-heal: drop orphan ArenaFS mounts / stale runsc sandboxes left by prior crashes.
	if n, err := arenaMgr.HealOrphans(ctx, *runscWrapper); err != nil {
		log.Printf("kontinued orphan heal warning: %v", err)
	} else if n > 0 {
		log.Printf("kontinued healed %d orphan arena/runsc artifacts on boot", n)
	}

	grpcLis, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Fatalf("listen gRPC %s: %v", *grpcListen, err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(telemetry.UnaryServerInterceptor("kontinued")),
		grpc.StreamInterceptor(telemetry.StreamServerInterceptor("kontinued")),
	)
	protocol.RegisterNodeControlServer(grpcSrv, svc)
	protocol.RegisterNodeArenaP2PServer(grpcSrv, p2p.NewServer(arenaMgr))

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc(protocol.PathDebugTraces, prov.Recorder().Handler())
	httpSrv := &http.Server{Addr: *httpListen, Handler: mux}

	// Periodic node heartbeat reporting L1/L2 cache inventory and CPU compatibility fingerprint.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		publish := func() {
			l1, l2 := arenaMgr.Inventory()
			_ = catalog.UpsertNodeHeartbeat(ctx, protocol.NodeHeartbeat{
				NodeID:          *nodeID,
				NodeIP:          *nodeIP,
				GRPCAddr:        advertiseGRPC,
				P2PAddr:         advertiseGRPC,
				KubeletPort:     10250,
				TotalMemBytes:   16 << 30,
				FreeMemBytes:    12 << 30,
				ActiveSandboxes: svc.ActiveCount(),
				L1Snapshots:     l1,
				L2Snapshots:     l2,
				Fingerprint:     fp,
			}, 20*time.Second)
		}
		publish()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				publish()
			}
		}
	}()

	go func() { _ = httpSrv.ListenAndServe() }()
	go func() {
		log.Printf("kontinued started on gRPC %s, HTTP %s (arch=%s cpuProfile=%s)", *grpcListen, *httpListen, fp.Architecture, fp.CPUProfileHash)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	<-ctx.Done()
	grpcSrv.GracefulStop()
	_ = httpSrv.Close()
}
