package crishim_test

import (
	"context"
	"net"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/crishim"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeUpstreamRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	containers []*runtimeapi.Container
}

func (f *fakeUpstreamRuntime) ListContainers(_ context.Context, _ *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	return &runtimeapi.ListContainersResponse{Containers: f.containers}, nil
}

func (f *fakeUpstreamRuntime) ContainerStatus(_ context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	for _, c := range f.containers {
		if c.Id == req.ContainerId {
			return &runtimeapi.ContainerStatusResponse{
				Status: &runtimeapi.ContainerStatus{
					Id:     c.Id,
					Labels: c.Labels,
					State:  c.State,
				},
			}, nil
		}
	}
	return &runtimeapi.ContainerStatusResponse{}, nil
}

type fakeNodeCtrl struct {
	lastExec *protocol.ExecuteCheckpointRequest
}

func (f *fakeNodeCtrl) PrepareCheckpoint(_ context.Context, _ *protocol.PrepareCheckpointRequest) (*protocol.PrepareCheckpointResponse, error) {
	return &protocol.PrepareCheckpointResponse{Accepted: true}, nil
}
func (f *fakeNodeCtrl) ExecuteCheckpoint(_ context.Context, req *protocol.ExecuteCheckpointRequest) (*protocol.ExecuteCheckpointResponse, error) {
	f.lastExec = req
	return &protocol.ExecuteCheckpointResponse{}, nil
}
func (f *fakeNodeCtrl) HydrateSlot(_ context.Context, _ *protocol.HydrateSlotRequest) (*protocol.HydrateSlotResponse, error) {
	return &protocol.HydrateSlotResponse{}, nil
}
func (f *fakeNodeCtrl) ReleaseSlot(_ context.Context, _ *protocol.ReleaseSlotRequest) (*protocol.ReleaseSlotResponse, error) {
	return &protocol.ReleaseSlotResponse{}, nil
}
func (f *fakeNodeCtrl) PrewarmTemplate(_ context.Context, _ *protocol.PrewarmTemplateRequest) (*protocol.PrewarmTemplateResponse, error) {
	return &protocol.PrewarmTemplateResponse{}, nil
}
func (f *fakeNodeCtrl) GetLatestSnapshotResult(_ context.Context, _ *protocol.GetLatestSnapshotResultRequest) (*protocol.GetLatestSnapshotResultResponse, error) {
	return &protocol.GetLatestSnapshotResultResponse{}, nil
}
func (f *fakeNodeCtrl) ResyncIdleCarriers(_ context.Context, _ *protocol.ResyncIdleCarriersRequest) (*protocol.ResyncIdleCarriersResponse, error) {
	return &protocol.ResyncIdleCarriersResponse{Synced: true}, nil
}

func TestCRIShimPLEGHashAlignmentAndCheckpointInterception(t *testing.T) {
	upLis, _ := net.Listen("tcp", "127.0.0.1:0")
	upSrv := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(upSrv, &fakeUpstreamRuntime{
		containers: []*runtimeapi.Container{
			{Id: "placeholder-ctr-1", PodSandboxId: "pod-sbx-1", State: runtimeapi.ContainerState_CONTAINER_EXITED},
			{Id: "restored-gvisor-ctr-1", PodSandboxId: "pod-sbx-1", State: runtimeapi.ContainerState_CONTAINER_RUNNING, Labels: map[string]string{}},
		},
	})
	go func() { _ = upSrv.Serve(upLis) }()
	defer upSrv.Stop()

	nodeLis, _ := net.Listen("tcp", "127.0.0.1:0")
	nodeSrv := grpc.NewServer()
	nodeImpl := &fakeNodeCtrl{}
	protocol.RegisterNodeControlServer(nodeSrv, nodeImpl)
	go func() { _ = nodeSrv.Serve(nodeLis) }()
	defer nodeSrv.Stop()

	upCC, _ := grpc.NewClient(upLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer upCC.Close()
	nodeCC, _ := grpc.NewClient(nodeLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer nodeCC.Close()

	mapper := crishim.NewMapper()
	mapper.Upsert(crishim.SlotMapping{
		PodSandboxID:          "pod-sbx-1",
		PodNamespace:          "default",
		PodName:               "carrier-pool-abc",
		PlaceholderID:         "placeholder-ctr-1",
		ActiveContainerID:     "restored-gvisor-ctr-1",
		ExpectedContainerHash: "kubelet-hash-98765",
	})

	proxy := crishim.NewProxy(upCC, protocol.NewNodeControlClient(nodeCC), mapper)
	ctx := context.Background()

	// 1. ListContainers hides exited placeholder and stamps kubelet-hash-98765 on restored container!
	listResp, err := proxy.ListContainers(ctx, &runtimeapi.ListContainersRequest{})
	if err != nil {
		t.Fatalf("ListContainers: %v", err)
	}
	if len(listResp.Containers) != 1 || listResp.Containers[0].Id != "restored-gvisor-ctr-1" {
		t.Fatalf("expected only restored container, got %+v", listResp.Containers)
	}
	if gotHash := listResp.Containers[0].Labels[protocol.K8sLabelContainerHash]; gotHash != "kubelet-hash-98765" {
		t.Fatalf("expected PLEG container hash kubelet-hash-98765, got %q", gotHash)
	}

	// 2. CheckpointContainer intercepts Kubelet /checkpoint call and forwards resolved active container ID!
	_, err = proxy.CheckpointContainer(ctx, &runtimeapi.CheckpointContainerRequest{
		ContainerId: "placeholder-ctr-1",
		Location:    "/var/lib/kubelet/checkpoints/ckpt.tar",
	})
	if err != nil {
		t.Fatalf("CheckpointContainer: %v", err)
	}
	if nodeImpl.lastExec == nil || nodeImpl.lastExec.ContainerID != "restored-gvisor-ctr-1" {
		t.Fatalf("expected checkpoint forwarded for restored-gvisor-ctr-1, got %+v", nodeImpl.lastExec)
	}
}
