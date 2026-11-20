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

	"github.com/kontinue-sh/kontinue/internal/gateway"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
)

func main() {
	var (
		listenAddr = flag.String("listen", ":8080", "Edge HTTP/WebSocket proxy listen address")
		apiURL     = flag.String("apiserver-url", getEnv("KONTINUE_APISERVER_URL", "http://127.0.0.1:8081"), "Kontinue API Server base URL")
		redisAddr  = flag.String("redis-addr", os.Getenv("REDIS_ADDR"), "Redis address")
		baseDomain = flag.String("base-domain", getEnv("KONTINUE_BASE_DOMAIN", "sandbox.local"), "Wildcard sandbox domain")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	prov, err := telemetry.Setup(ctx, telemetry.Config{ServiceName: "kontinue-gateway"})
	if err != nil {
		log.Fatalf("telemetry setup: %v", err)
	}
	defer func() { _ = prov.Shutdown(context.Background()) }()

	var catalog store.RuntimeCatalog
	if *redisAddr != "" {
		rd := store.NewRedisRuntimeCatalog(*redisAddr, os.Getenv("REDIS_PASSWORD"), 0)
		defer rd.Close()
		catalog = rd
	} else {
		catalog = store.NewMemoryRuntimeCatalog()
	}

	gw := gateway.NewGateway(gateway.Config{
		Catalog:      catalog,
		APIServerURL: *apiURL,
		BaseDomain:   *baseDomain,
		Recorder:     prov.Recorder(),
	})

	httpSrv := &http.Server{
		Addr:              *listenAddr,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("kontinue-gateway listening on %s -> apiserver %s", *listenAddr, *apiURL)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway error: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
