package crishim

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// SlotMapping tracks a hydrated Carrier Pod's container replacement so Kubelet PLEG
// sees a steady running container with its expected io.kubernetes.container.hash.
type SlotMapping struct {
	PodSandboxID          string `json:"podSandboxId"`
	PodNamespace          string `json:"podNamespace"`
	PodName               string `json:"podName"`
	PlaceholderID         string `json:"placeholderId"`
	ActiveContainerID     string `json:"activeContainerId"`
	ExpectedContainerHash string `json:"expectedContainerHash"`
	LogPath               string `json:"logPath,omitempty"`
}

// Mapper maintains thread-safe container identity translations for hydrated Carrier Pods.
type Mapper struct {
	mu          sync.RWMutex
	bySandbox   map[string]SlotMapping
	byActive    map[string]SlotMapping
	hiddenPlace map[string]SlotMapping
}

func NewMapper() *Mapper {
	return &Mapper{
		bySandbox:   make(map[string]SlotMapping),
		byActive:    make(map[string]SlotMapping),
		hiddenPlace: make(map[string]SlotMapping),
	}
}

func (m *Mapper) Upsert(mapping SlotMapping) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if prev, ok := m.bySandbox[mapping.PodSandboxID]; ok {
		delete(m.byActive, prev.ActiveContainerID)
		delete(m.hiddenPlace, prev.PlaceholderID)
	}
	m.bySandbox[mapping.PodSandboxID] = mapping
	if mapping.ActiveContainerID != "" {
		m.byActive[mapping.ActiveContainerID] = mapping
	}
	if mapping.PlaceholderID != "" && mapping.PlaceholderID != mapping.ActiveContainerID {
		m.hiddenPlace[mapping.PlaceholderID] = mapping
	}
}

func (m *Mapper) RemoveBySandbox(podSandboxID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if prev, ok := m.bySandbox[podSandboxID]; ok {
		delete(m.byActive, prev.ActiveContainerID)
		delete(m.hiddenPlace, prev.PlaceholderID)
		delete(m.bySandbox, podSandboxID)
	}
}

func (m *Mapper) IsHiddenPlaceholder(containerID string) (SlotMapping, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.hiddenPlace[containerID]
	return v, ok
}

func (m *Mapper) LookupActive(containerID string) (SlotMapping, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.byActive[containerID]; ok {
		return v, true
	}
	if v, ok := m.hiddenPlace[containerID]; ok {
		return v, true
	}
	return SlotMapping{}, false
}

// HTTPHandler exposes local Unix-socket control endpoints (/v1/shim/map-slot) for kontinued.
func (m *Mapper) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/shim/map-slot", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			var sm SlotMapping
			if err := json.NewDecoder(r.Body).Decode(&sm); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			m.Upsert(sm)
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			sbxID := r.URL.Query().Get("pod_sandbox_id")
			m.RemoveBySandbox(sbxID)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return mux
}

// Proxy implements CRI RuntimeServiceServer & ImageServiceServer with zero-modification passthrough
// for regular workloads, PLEG hash alignment for Carrier Pods, and CheckpointContainer interception.
type Proxy struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	runtimeapi.UnimplementedImageServiceServer

	upstreamRuntime runtimeapi.RuntimeServiceClient
	upstreamImage   runtimeapi.ImageServiceClient
	nodeCtrl        *protocol.NodeControlClient
	mapper          *Mapper
}

func NewProxy(upstreamConn *grpc.ClientConn, nodeCtrl *protocol.NodeControlClient, mapper *Mapper) *Proxy {
	if mapper == nil {
		mapper = NewMapper()
	}
	return &Proxy{
		upstreamRuntime: runtimeapi.NewRuntimeServiceClient(upstreamConn),
		upstreamImage:   runtimeapi.NewImageServiceClient(upstreamConn),
		nodeCtrl:        nodeCtrl,
		mapper:          mapper,
	}
}

func NewProxyFromClients(rt runtimeapi.RuntimeServiceClient, img runtimeapi.ImageServiceClient, nodeCtrl *protocol.NodeControlClient, mapper *Mapper) *Proxy {
	if mapper == nil {
		mapper = NewMapper()
	}
	return &Proxy{
		upstreamRuntime: rt,
		upstreamImage:   img,
		nodeCtrl:        nodeCtrl,
		mapper:          mapper,
	}
}

func (p *Proxy) Mapper() *Mapper { return p.mapper }

// CheckpointContainer intercepts Kubelet's official POST /checkpoint/{ns}/{pod}/{container} CRI RPC
// and delegates gVisor split checkpointing + 2MiB Blake3 chunking to kontinued.
func (p *Proxy) CheckpointContainer(ctx context.Context, req *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-cri-shim", "crishim.checkpoint_container",
		attribute.String("cri.container_id", req.ContainerId),
		attribute.String("cri.location", req.Location),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	targetCID := req.ContainerId
	var ns, podName string
	if sm, ok := p.mapper.LookupActive(req.ContainerId); ok {
		if sm.ActiveContainerID != "" {
			targetCID = sm.ActiveContainerID
		}
		ns = sm.PodNamespace
		podName = sm.PodName
	}

	if p.nodeCtrl == nil {
		return p.upstreamRuntime.CheckpointContainer(ctx, req)
	}
	_, retErr = p.nodeCtrl.ExecuteCheckpoint(ctx, &protocol.ExecuteCheckpointRequest{
		ContainerID:     targetCID,
		PodNamespace:    ns,
		PodName:         podName,
		KubeletLocation: req.Location,
		TimeoutSeconds:  req.Timeout,
	})
	if retErr != nil {
		return nil, retErr
	}
	return &runtimeapi.CheckpointContainerResponse{}, nil
}

// ListContainers hides replaced Carrier Pod placeholder containers and aligns io.kubernetes.container.hash
// so Kubelet PLEG never triggers unwanted container restarts after HydrateSlot.
func (p *Proxy) ListContainers(ctx context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	resp, err := p.upstreamRuntime.ListContainers(ctx, req)
	if err != nil {
		return nil, err
	}
	presentInCRI := make(map[string]bool, len(resp.Containers))
	for _, ctr := range resp.Containers {
		presentInCRI[ctr.Id] = true
	}
	filtered := make([]*runtimeapi.Container, 0, len(resp.Containers))
	for _, ctr := range resp.Containers {
		// Only hide placeholder if the replacement container itself is managed inside CRI (subcontainer-swap mode).
		// In standalone GVisorSandboxDriver mode, keeping the running placeholder visible keeps Kubelet PLEG & /checkpoint happy!
		if sm, hidden := p.mapper.IsHiddenPlaceholder(ctr.Id); hidden && presentInCRI[sm.ActiveContainerID] {
			continue
		}
		if sm, ok := p.mapper.LookupActive(ctr.Id); ok && sm.ExpectedContainerHash != "" && ctr.Id == sm.ActiveContainerID {
			if ctr.Labels == nil {
				ctr.Labels = make(map[string]string)
			}
			ctr.Labels[protocol.K8sLabelContainerHash] = sm.ExpectedContainerHash
		}
		filtered = append(filtered, ctr)
	}
	resp.Containers = filtered
	return resp, nil
}

func (p *Proxy) ContainerStatus(ctx context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	resp, err := p.upstreamRuntime.ContainerStatus(ctx, req)
	if err != nil {
		if sm, hidden := p.mapper.IsHiddenPlaceholder(req.ContainerId); hidden && sm.ActiveContainerID != "" {
			resp, err = p.upstreamRuntime.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{
				ContainerId: sm.ActiveContainerID,
				Verbose:     req.Verbose,
			})
		}
	}
	if err != nil {
		return nil, err
	}
	if resp.Status != nil {
		if sm, ok := p.mapper.LookupActive(resp.Status.Id); ok {
			if sm.ExpectedContainerHash != "" {
				if resp.Status.Labels == nil {
					resp.Status.Labels = make(map[string]string)
				}
				resp.Status.Labels[protocol.K8sLabelContainerHash] = sm.ExpectedContainerHash
			}
			if sm.LogPath != "" && resp.Status.LogPath == "" {
				resp.Status.LogPath = sm.LogPath
			}
		}
	}
	return resp, nil
}

// --- Transparent Passthroughs for Remaining CRI RuntimeService & ImageService RPCs ---

func (p *Proxy) Version(ctx context.Context, r *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	return p.upstreamRuntime.Version(ctx, r)
}
func (p *Proxy) RunPodSandbox(ctx context.Context, r *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	return p.upstreamRuntime.RunPodSandbox(ctx, r)
}
func (p *Proxy) StopPodSandbox(ctx context.Context, r *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	p.mapper.RemoveBySandbox(r.PodSandboxId)
	return p.upstreamRuntime.StopPodSandbox(ctx, r)
}
func (p *Proxy) RemovePodSandbox(ctx context.Context, r *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	p.mapper.RemoveBySandbox(r.PodSandboxId)
	return p.upstreamRuntime.RemovePodSandbox(ctx, r)
}
func (p *Proxy) PodSandboxStatus(ctx context.Context, r *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	return p.upstreamRuntime.PodSandboxStatus(ctx, r)
}
func (p *Proxy) ListPodSandbox(ctx context.Context, r *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	return p.upstreamRuntime.ListPodSandbox(ctx, r)
}
func (p *Proxy) CreateContainer(ctx context.Context, r *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	return p.upstreamRuntime.CreateContainer(ctx, r)
}
func (p *Proxy) StartContainer(ctx context.Context, r *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	return p.upstreamRuntime.StartContainer(ctx, r)
}
func (p *Proxy) StopContainer(ctx context.Context, r *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	return p.upstreamRuntime.StopContainer(ctx, r)
}
func (p *Proxy) RemoveContainer(ctx context.Context, r *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	return p.upstreamRuntime.RemoveContainer(ctx, r)
}
func (p *Proxy) UpdateContainerResources(ctx context.Context, r *runtimeapi.UpdateContainerResourcesRequest) (*runtimeapi.UpdateContainerResourcesResponse, error) {
	return p.upstreamRuntime.UpdateContainerResources(ctx, r)
}
func (p *Proxy) ReopenContainerLog(ctx context.Context, r *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	return p.upstreamRuntime.ReopenContainerLog(ctx, r)
}
func (p *Proxy) ExecSync(ctx context.Context, r *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	return p.upstreamRuntime.ExecSync(ctx, r)
}
func (p *Proxy) Exec(ctx context.Context, r *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	return p.upstreamRuntime.Exec(ctx, r)
}
func (p *Proxy) Attach(ctx context.Context, r *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	return p.upstreamRuntime.Attach(ctx, r)
}
func (p *Proxy) PortForward(ctx context.Context, r *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	return p.upstreamRuntime.PortForward(ctx, r)
}
func (p *Proxy) ContainerStats(ctx context.Context, r *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	return p.upstreamRuntime.ContainerStats(ctx, r)
}
func (p *Proxy) ListContainerStats(ctx context.Context, r *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	return p.upstreamRuntime.ListContainerStats(ctx, r)
}
func (p *Proxy) PodSandboxStats(ctx context.Context, r *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	return p.upstreamRuntime.PodSandboxStats(ctx, r)
}
func (p *Proxy) ListPodSandboxStats(ctx context.Context, r *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	return p.upstreamRuntime.ListPodSandboxStats(ctx, r)
}
func (p *Proxy) UpdateRuntimeConfig(ctx context.Context, r *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	return p.upstreamRuntime.UpdateRuntimeConfig(ctx, r)
}
func (p *Proxy) Status(ctx context.Context, r *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	return p.upstreamRuntime.Status(ctx, r)
}
func (p *Proxy) RuntimeConfig(ctx context.Context, r *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	return p.upstreamRuntime.RuntimeConfig(ctx, r)
}

func (p *Proxy) ListImages(ctx context.Context, r *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	return p.upstreamImage.ListImages(ctx, r)
}
func (p *Proxy) ImageStatus(ctx context.Context, r *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	return p.upstreamImage.ImageStatus(ctx, r)
}
func (p *Proxy) PullImage(ctx context.Context, r *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	return p.upstreamImage.PullImage(ctx, r)
}
func (p *Proxy) RemoveImage(ctx context.Context, r *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	return p.upstreamImage.RemoveImage(ctx, r)
}
func (p *Proxy) ImageFsInfo(ctx context.Context, r *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	return p.upstreamImage.ImageFsInfo(ctx, r)
}
