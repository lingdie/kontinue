package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kontinue-sh/kontinue/internal/apiserver"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	var (
		listenAddr      = flag.String("listen", ":8081", "HTTP API listen address")
		pgDSN           = flag.String("postgres-dsn", os.Getenv("POSTGRES_DSN"), "PostgreSQL connection string (empty uses in-memory store)")
		redisAddr       = flag.String("redis-addr", os.Getenv("REDIS_ADDR"), "Redis address (empty uses in-memory catalog)")
		namespace       = flag.String("carrier-namespace", "default", "Namespace hosting pre-warmed Carrier Pods")
		carrierImg      = flag.String("carrier-image", "kontinue/carrier:dev", "Placeholder container image for Carrier Pods")
		minIdleSlots    = flag.Int("min-idle-slots", 4, "Minimum pre-warmed Carrier Pods maintained per node")
		idleBurstBuffer = flag.Int("idle-burst-buffer", 2, "Extra idle Carrier Pods above min-idle maintained asynchronously")
		coldBurstExtra  = flag.Int("cold-burst-extra", 1, "Extra Carrier Pods created on each cold allocate")
		kubeletInsec    = flag.Bool("kubelet-insecure-tls", true, "Skip Kubelet :10250 TLS cert verification in dev/Kind clusters")
		casURL          = flag.String("cas-url", os.Getenv("CAS_HTTP_URL"), "CAS HTTP base URL for physical snapshot GC")
		localCASRoot    = flag.String("local-cas-fallback", os.Getenv("LOCAL_CAS_ROOT"), "Local/shared CAS root for physical GC")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	prov, err := telemetry.Setup(ctx, telemetry.Config{ServiceName: "kontinue-apiserver"})
	if err != nil {
		log.Fatalf("telemetry setup: %v", err)
	}
	defer func() { _ = prov.Shutdown(context.Background()) }()

	var metaStore store.MetadataStore
	if *pgDSN != "" {
		pgStore, pgErr := store.NewPostgresMetadataStore(ctx, *pgDSN)
		if pgErr != nil {
			log.Fatalf("connect postgres: %v", pgErr)
		}
		defer pgStore.Close()
		metaStore = pgStore
	} else {
		metaStore = store.NewMemoryMetadataStore()
	}
	if err := metaStore.Migrate(ctx); err != nil {
		log.Fatalf("migrate metadata store: %v", err)
	}

	var catalog store.RuntimeCatalog
	if *redisAddr != "" {
		rd := store.NewRedisRuntimeCatalog(*redisAddr, os.Getenv("REDIS_PASSWORD"), 0)
		defer rd.Close()
		catalog = rd
	} else {
		catalog = store.NewMemoryRuntimeCatalog()
	}

	var kubeClient kubernetes.Interface
	if kCfg, kErr := rest.InClusterConfig(); kErr == nil {
		kCfg.QPS = 200
		kCfg.Burst = 400
		kubeClient, _ = kubernetes.NewForConfig(kCfg)
	}

	resyncNode := func(ctx context.Context, nodeGRPCAddr string) error {
		if nodeGRPCAddr == "" {
			return nil
		}
		cc, err := grpc.NewClient(nodeGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer cc.Close()
		cli := protocol.NewNodeControlClient(cc)
		_, err = cli.ResyncIdleCarriers(ctx, &protocol.ResyncIdleCarriersRequest{})
		return err
	}

	replenisher := apiserver.NewCarrierPoolReplenisher(kubeClient, catalog, apiserver.ReplenisherConfig{
		Namespace:       *namespace,
		CarrierImage:    *carrierImg,
		Leader:          metaStore,
		MinIdlePerNode:  *minIdleSlots,
		IdleBurstBuffer: *idleBurstBuffer,
		ColdBurstExtra:  *coldBurstExtra,
		ResyncNode:      resyncNode,
	})
	replenisher.Start(ctx)

	var casBackend storage.Backend
	if *casURL != "" {
		casBackend = storage.NewHTTPObjectStoreBackend(*casURL, os.Getenv("CAS_AUTH_TOKEN"), nil)
	} else if *localCASRoot != "" {
		if b, bErr := storage.NewLocalFSBackend(*localCASRoot); bErr == nil {
			casBackend = b
		}
	}

	kubelet := apiserver.NewHTTPSKubeletClient(os.Getenv("KUBELET_BEARER_TOKEN"), *kubeletInsec)
	srv := apiserver.NewServer(apiserver.ServerConfig{
		Meta:        metaStore,
		Catalog:     catalog,
		Replenisher: replenisher,
		Kubelet:     kubelet,
		Recorder:    prov.Recorder(),
		CAS:         casBackend,
	})
	defer srv.Close()
	srv.StartBackgroundWorkers(ctx, 5*time.Second)

	httpSrv := &http.Server{
		Addr:              *listenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("kontinue-apiserver listening on %s", *listenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("apiserver error: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
}
