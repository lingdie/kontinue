package apiserver

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func TestAssignedWorkerCanReconnectOnTheSameAddress(t *testing.T) {
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listen.Close()
	api := &Server{nodeConns: make(map[string]*grpc.ClientConn)}
	address := listen.Addr().String()
	conn, err := api.nodeConnection(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The address is valid, but the replacement worker cannot serve RPC yet.
	unavailable, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = api.nodeClient(unavailable, address)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("returned before the worker was ready: %v", err)
	}

	worker := grpc.NewServer()
	defer worker.Stop()
	go func() { _ = worker.Serve(listen) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := api.nodeClient(ctx, address); err != nil {
		t.Fatal(err)
	}
	if conn.GetState() != connectivity.Ready {
		t.Fatal("assigned connection did not recover")
	}
}
