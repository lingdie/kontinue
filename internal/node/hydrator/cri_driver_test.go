package hydrator

import (
	"context"
	"errors"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type teardownRuntime struct {
	runtimeapi.RuntimeServiceClient
	stopErr   error
	removeErr error
	stops     int
	removes   int
}

func (r *teardownRuntime) StopContainer(context.Context, *runtimeapi.StopContainerRequest, ...grpc.CallOption) (*runtimeapi.StopContainerResponse, error) {
	r.stops++
	return nil, r.stopErr
}

func (r *teardownRuntime) RemoveContainer(context.Context, *runtimeapi.RemoveContainerRequest, ...grpc.CallOption) (*runtimeapi.RemoveContainerResponse, error) {
	r.removes++
	return nil, r.removeErr
}

func (r *teardownRuntime) CreateContainer(context.Context, *runtimeapi.CreateContainerRequest, ...grpc.CallOption) (*runtimeapi.CreateContainerResponse, error) {
	return &runtimeapi.CreateContainerResponse{ContainerId: "created"}, nil
}

func (r *teardownRuntime) StartContainer(context.Context, *runtimeapi.StartContainerRequest, ...grpc.CallOption) (*runtimeapi.StartContainerResponse, error) {
	return &runtimeapi.StartContainerResponse{}, nil
}

func TestCRIRuntimeDriverPreservesTeardownFailuresForRetry(t *testing.T) {
	stopErr := status.Error(codes.Unavailable, "runtime is restarting")
	removeErr := status.Error(codes.Unavailable, "remove is unavailable")
	tests := []struct {
		name         string
		runtime      *teardownRuntime
		wantErr      error
		wantStops    int
		wantRemovals int
	}{
		{name: "stop failure", runtime: &teardownRuntime{stopErr: stopErr}, wantErr: stopErr, wantStops: 1},
		{name: "remove failure", runtime: &teardownRuntime{removeErr: removeErr}, wantErr: removeErr, wantStops: 1, wantRemovals: 1},
		{name: "already gone", runtime: &teardownRuntime{stopErr: status.Error(codes.NotFound, "missing"), removeErr: status.Error(codes.NotFound, "missing")}, wantStops: 1, wantRemovals: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewCRIRuntimeDriver(tc.runtime, nil, "", "")
			err := d.StopSandboxContainer(context.Background(), protocol.CarrierSlot{}, "container")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error=%v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected teardown error: %v", err)
			}
			if tc.runtime.stops != tc.wantStops || tc.runtime.removes != tc.wantRemovals {
				t.Fatalf("stop calls=%d remove calls=%d, want %d/%d", tc.runtime.stops, tc.runtime.removes, tc.wantStops, tc.wantRemovals)
			}
		})
	}
}

func TestCRIRuntimeDriverDoesNotStartBesideAnUnremovedPlaceholder(t *testing.T) {
	runtime := &teardownRuntime{stopErr: status.Error(codes.Unavailable, "runtime is restarting")}
	d := NewCRIRuntimeDriver(runtime, nil, "", "")
	_, _, err := d.SwapAndStartSandboxContainer(context.Background(), SwapContainerOptions{
		ContainerID: "kt-active", SandboxID: "box", Slot: protocol.CarrierSlot{PlaceholderID: "placeholder"},
	})
	if err == nil {
		t.Fatal("started a replacement while the placeholder could not be stopped")
	}
	if runtime.removes != 0 {
		t.Fatalf("removed placeholder after stop failure: %d calls", runtime.removes)
	}
}
