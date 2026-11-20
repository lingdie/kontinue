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

	"github.com/kontinue-sh/kontinue/internal/crishim"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func main() {
	var (
		listenSock   = flag.String("listen-sock", "/run/kontinue/cri.sock", "CRI Unix socket exposed to Kubelet")
		upstreamSock = flag.String("upstream-sock", "/run/containerd/containerd.sock", "Upstream containerd CRI Unix socket")
		nodeGRPCAddr = flag.String("node-grpc", "127.0.0.1:9090", "Local kontinued NodeControlService gRPC address")
		adminSock    = flag.String("admin-sock", "/run/kontinue/cri-shim-admin.sock", "Local Unix socket for SlotMapper updates")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	prov, _ := telemetry.Setup(ctx, telemetry.Config{ServiceName: "kontinue-cri-shim"})
	defer func() { _ = prov.Shutdown(context.Background()) }()

	upstreamConn, err := grpc.NewClient("unix://"+*upstreamSock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(telemetry.UnaryClientInterceptor("kontinue-cri-shim")),
	)
	if err != nil {
		log.Fatalf("dial containerd %s: %v", *upstreamSock, err)
	}
	defer upstreamConn.Close()

	nodeConn, err := grpc.NewClient(*nodeGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(telemetry.UnaryClientInterceptor("kontinue-cri-shim")),
	)
	if err != nil {
		log.Fatalf("dial kontinued %s: %v", *nodeGRPCAddr, err)
	}
	defer nodeConn.Close()

	mapper := crishim.NewMapper()
	proxy := crishim.NewProxy(upstreamConn, protocol.NewNodeControlClient(nodeConn), mapper)

	for _, p := range []string{*listenSock, *adminSock} {
		_ = os.MkdirAll(filepath.Dir(p), 0o700)
		_ = os.Remove(p)
	}

	criLis, err := net.Listen("unix", *listenSock)
	if err != nil {
		log.Fatalf("listen %s: %v", *listenSock, err)
	}
	adminLis, err := net.Listen("unix", *adminSock)
	if err != nil {
		log.Fatalf("listen %s: %v", *adminSock, err)
	}

	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(telemetry.UnaryServerInterceptor("kontinue-cri-shim")))
	runtimeapi.RegisterRuntimeServiceServer(grpcSrv, proxy)
	runtimeapi.RegisterImageServiceServer(grpcSrv, proxy)

	mux := http.NewServeMux()
	mux.Handle("/v1/shim/", mapper.HTTPHandler())
	mux.HandleFunc(protocol.PathDebugTraces, prov.Recorder().Handler())
	httpSrv := &http.Server{Handler: mux}

	go func() { _ = httpSrv.Serve(adminLis) }()
	go func() {
		log.Printf("kontinue-cri-shim listening on unix://%s -> unix://%s", *listenSock, *upstreamSock)
		if err := grpcSrv.Serve(criLis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	<-ctx.Done()
	grpcSrv.GracefulStop()
	_ = httpSrv.Close()
}
