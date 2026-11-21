package protocol

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const JSONCodecName = "kontinue-json"

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return JSONCodecName }

// CarrierSlot describes an idle or active pre-warmed Kubernetes Carrier Pod slot on a worker node.
type CarrierSlot struct {
	SlotID                string `json:"slotId"`
	NodeID                string `json:"nodeId"`
	NodeIP                string `json:"nodeIp"`
	NodeGRPCAddr          string `json:"nodeGrpcAddr"`
	Tier                  string `json:"tier"`
	PodNamespace          string `json:"podNamespace"`
	PodName               string `json:"podName"`
	PodUID                string `json:"podUid"`
	PodIP                 string `json:"podIp"`
	PodSandboxID          string `json:"podSandboxId"`
	NetNSPath             string `json:"netnsPath,omitempty"`
	PlaceholderID         string `json:"placeholderContainerId"`
	ExpectedContainerHash string `json:"expectedContainerHash"`
	LogPath               string `json:"logPath,omitempty"`
	NetworkReadyOnly      bool   `json:"networkReadyOnly,omitempty"`
}

// RouteRecord is stored at kt:route:{sandbox_id} in Redis, guarded by Lua ExecutionID fencing.
type RouteRecord struct {
	AccessDigest   string    `json:"accessDigest,omitempty"`
	OwnerKey       string    `json:"ownerKey,omitempty"`
	SandboxID      string    `json:"sandboxId"`
	ExecutionID    int64     `json:"executionId"`
	LifecycleID    string    `json:"lifecycleId"`
	NodeID         string    `json:"nodeId"`
	NodeIP         string    `json:"nodeIp"`
	NodeGRPCAddr   string    `json:"nodeGrpcAddr"`
	SlotID         string    `json:"slotId"`
	CarrierPodName string    `json:"carrierPodName"`
	PodNamespace   string    `json:"podNamespace"`
	PodIP          string    `json:"podIp"`
	StartedAt      time.Time `json:"startedAt"`
}

// NodeHeartbeat is published periodically by kontinued into Redis for Best-of-K placement.
type NodeHeartbeat struct {
	NodeID          string                   `json:"nodeId"`
	NodeIP          string                   `json:"nodeIp"`
	GRPCAddr        string                   `json:"grpcAddr"`
	P2PAddr         string                   `json:"p2pAddr"`
	KubeletPort     int                      `json:"kubeletPort"`
	TotalMemBytes   int64                    `json:"totalMemBytes"`
	FreeMemBytes    int64                    `json:"freeMemBytes"`
	ActiveSandboxes int                      `json:"activeSandboxes"`
	L1Snapshots     []string                 `json:"l1Snapshots,omitempty"`
	L2Snapshots     []string                 `json:"l2Snapshots,omitempty"`
	Fingerprint     CompatibilityFingerprint `json:"fingerprint"`
	UpdatedAt       time.Time                `json:"updatedAt"`
}

// --- NodeControlService RPC messages (apiserver / cri-shim -> kontinued) ---

type PrepareCheckpointRequest struct {
	ExecutionID      int64        `json:"executionId,omitempty"`
	LifecycleID      string       `json:"lifecycleId,omitempty"`
	SandboxID        string       `json:"sandboxId"`
	TemplateID       string       `json:"templateId,omitempty"`
	BaseImage        string       `json:"baseImage"`
	SlotID           string       `json:"slotId"`
	PodNamespace     string       `json:"podNamespace"`
	PodName          string       `json:"podName"`
	PodIP            string       `json:"podIp"`
	TargetSnapshotID string       `json:"targetSnapshotId"`
	ParentSnapshotID string       `json:"parentSnapshotId,omitempty"`
	Kind             SnapshotKind `json:"kind"`
	LeaveRunning     bool         `json:"leaveRunning"`
	TraceHotPages    bool         `json:"traceHotPages"`
	BootstrapToken   string       `json:"bootstrapToken"`
}

type PrepareCheckpointResponse struct {
	Accepted bool `json:"accepted"`
}

type ExecuteCheckpointRequest struct {
	ContainerID     string `json:"containerId"`
	PodNamespace    string `json:"podNamespace,omitempty"`
	PodName         string `json:"podName,omitempty"`
	KubeletLocation string `json:"kubeletLocation"`
	TimeoutSeconds  int64  `json:"timeoutSeconds,omitempty"`
}

type ExecuteCheckpointResponse struct {
	Manifest    SnapshotManifest `json:"manifest"`
	ManifestURI string           `json:"manifestUri"`
	ArenaTier   string           `json:"arenaTier"`
}

type HydrateSlotRequest struct {
	DeferRoutePublication bool              `json:"deferRoutePublication,omitempty"`
	Slot                  CarrierSlot       `json:"slot"`
	SandboxID             string            `json:"sandboxId"`
	TemplateID            string            `json:"templateId,omitempty"`
	ExecutionID           int64             `json:"executionId"`
	LifecycleID           string            `json:"lifecycleId"`
	SnapshotID            string            `json:"snapshotId,omitempty"`
	BaseImage             string            `json:"baseImage"`
	RestoreMode           string            `json:"restoreMode"`
	BootstrapToken        string            `json:"bootstrapToken"`
	AccessToken           string            `json:"accessToken"`
	CPUMillis             int64             `json:"cpuMillis,omitempty"`
	MemoryBytes           int64             `json:"memoryBytes,omitempty"`
	EnvVars               map[string]string `json:"envVars,omitempty"`
	SkipThaw              bool              `json:"skipThaw,omitempty"`
}

type HydrateSlotResponse struct {
	RestoreViewCopiedBytes *int64 `json:"restoreViewCopiedBytes,omitempty"`
	SandboxID              string `json:"sandboxId"`
	ExecutionID            int64  `json:"executionId"`
	PodIP                  string `json:"podIp"`
	ContainerID            string `json:"containerId"`
	ArenaTier              string `json:"arenaTier"`
	RestoreMode            string `json:"restoreMode"`
	DurationMs             int64  `json:"durationMs"`
}

type ReleaseSlotRequest struct {
	LifecycleID string `json:"lifecycleId,omitempty"`
	SlotID      string `json:"slotId"`
	SandboxID   string `json:"sandboxId"`
	PodUID      string `json:"podUid,omitempty"`
	ExecutionID int64  `json:"executionId"`
	ReuseSlot   bool   `json:"reuseSlot"`
}

type ReleaseSlotResponse struct {
	PodUID   string `json:"podUid,omitempty"`
	Released bool   `json:"released"`
	Tainted  bool   `json:"tainted"`
}

type PrewarmTemplateRequest struct {
	SnapshotID string `json:"snapshotId"`
	PinInTmpfs bool   `json:"pinInTmpfs"`
}

type PrewarmTemplateResponse struct {
	Ready          bool   `json:"ready"`
	LocalArenaPath string `json:"localArenaPath"`
	BytesPinned    int64  `json:"bytesPinned"`
}

type GetLatestSnapshotResultRequest struct {
	Expected   *PrepareCheckpointRequest `json:"expected,omitempty"`
	SnapshotID string                    `json:"snapshotId"`
}

type GetLatestSnapshotResultResponse struct {
	Found       bool             `json:"found"`
	Manifest    SnapshotManifest `json:"manifest"`
	ManifestURI string           `json:"manifestUri"`
}

type ResyncIdleCarriersRequest struct{}

type ResyncIdleCarriersResponse struct {
	Synced bool `json:"synced"`
}

type NodeControlServer interface {
	PrepareCheckpoint(context.Context, *PrepareCheckpointRequest) (*PrepareCheckpointResponse, error)
	ExecuteCheckpoint(context.Context, *ExecuteCheckpointRequest) (*ExecuteCheckpointResponse, error)
	HydrateSlot(context.Context, *HydrateSlotRequest) (*HydrateSlotResponse, error)
	ReleaseSlot(context.Context, *ReleaseSlotRequest) (*ReleaseSlotResponse, error)
	PrewarmTemplate(context.Context, *PrewarmTemplateRequest) (*PrewarmTemplateResponse, error)
	GetLatestSnapshotResult(context.Context, *GetLatestSnapshotResultRequest) (*GetLatestSnapshotResultResponse, error)
	ResyncIdleCarriers(context.Context, *ResyncIdleCarriersRequest) (*ResyncIdleCarriersResponse, error)
}

func unaryMethod[Req any, Resp any](fullMethod string, fn func(NodeControlServer, context.Context, *Req) (*Resp, error)) func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := new(Req)
		if err := dec(in); err != nil {
			return nil, err
		}
		s := srv.(NodeControlServer)
		if interceptor == nil {
			return fn(s, ctx, in)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: fullMethod}
		return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
			return fn(s, ctx, req.(*Req))
		})
	}
}

var NodeControlServiceDesc = grpc.ServiceDesc{
	ServiceName: "kontinue.node.v1.NodeControlService",
	HandlerType: (*NodeControlServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "PrepareCheckpoint", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/PrepareCheckpoint", NodeControlServer.PrepareCheckpoint)},
		{MethodName: "ExecuteCheckpoint", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/ExecuteCheckpoint", NodeControlServer.ExecuteCheckpoint)},
		{MethodName: "HydrateSlot", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/HydrateSlot", NodeControlServer.HydrateSlot)},
		{MethodName: "ReleaseSlot", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/ReleaseSlot", NodeControlServer.ReleaseSlot)},
		{MethodName: "PrewarmTemplate", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/PrewarmTemplate", NodeControlServer.PrewarmTemplate)},
		{MethodName: "GetLatestSnapshotResult", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/GetLatestSnapshotResult", NodeControlServer.GetLatestSnapshotResult)},
		{MethodName: "ResyncIdleCarriers", Handler: unaryMethod("/kontinue.node.v1.NodeControlService/ResyncIdleCarriers", NodeControlServer.ResyncIdleCarriers)},
	},
	Metadata: "kontinue/node_control.json",
}

func RegisterNodeControlServer(s grpc.ServiceRegistrar, srv NodeControlServer) {
	s.RegisterService(&NodeControlServiceDesc, srv)
}

type NodeControlClient struct {
	cc grpc.ClientConnInterface
}

func NewNodeControlClient(cc grpc.ClientConnInterface) *NodeControlClient {
	return &NodeControlClient{cc: cc}
}

func invokeJSON[Req any, Resp any](ctx context.Context, cc grpc.ClientConnInterface, method string, req *Req, opts ...grpc.CallOption) (*Resp, error) {
	opts = append([]grpc.CallOption{grpc.ForceCodec(jsonCodec{})}, opts...)
	out := new(Resp)
	if err := cc.Invoke(ctx, method, req, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *NodeControlClient) PrepareCheckpoint(ctx context.Context, req *PrepareCheckpointRequest, opts ...grpc.CallOption) (*PrepareCheckpointResponse, error) {
	return invokeJSON[PrepareCheckpointRequest, PrepareCheckpointResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/PrepareCheckpoint", req, opts...)
}

func (c *NodeControlClient) ExecuteCheckpoint(ctx context.Context, req *ExecuteCheckpointRequest, opts ...grpc.CallOption) (*ExecuteCheckpointResponse, error) {
	return invokeJSON[ExecuteCheckpointRequest, ExecuteCheckpointResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/ExecuteCheckpoint", req, opts...)
}

func (c *NodeControlClient) HydrateSlot(ctx context.Context, req *HydrateSlotRequest, opts ...grpc.CallOption) (*HydrateSlotResponse, error) {
	return invokeJSON[HydrateSlotRequest, HydrateSlotResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/HydrateSlot", req, opts...)
}

func (c *NodeControlClient) ReleaseSlot(ctx context.Context, req *ReleaseSlotRequest, opts ...grpc.CallOption) (*ReleaseSlotResponse, error) {
	return invokeJSON[ReleaseSlotRequest, ReleaseSlotResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/ReleaseSlot", req, opts...)
}

func (c *NodeControlClient) PrewarmTemplate(ctx context.Context, req *PrewarmTemplateRequest, opts ...grpc.CallOption) (*PrewarmTemplateResponse, error) {
	return invokeJSON[PrewarmTemplateRequest, PrewarmTemplateResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/PrewarmTemplate", req, opts...)
}

func (c *NodeControlClient) GetLatestSnapshotResult(ctx context.Context, req *GetLatestSnapshotResultRequest, opts ...grpc.CallOption) (*GetLatestSnapshotResultResponse, error) {
	return invokeJSON[GetLatestSnapshotResultRequest, GetLatestSnapshotResultResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/GetLatestSnapshotResult", req, opts...)
}

func (c *NodeControlClient) ResyncIdleCarriers(ctx context.Context, req *ResyncIdleCarriersRequest, opts ...grpc.CallOption) (*ResyncIdleCarriersResponse, error) {
	return invokeJSON[ResyncIdleCarriersRequest, ResyncIdleCarriersResponse](ctx, c.cc, "/kontinue.node.v1.NodeControlService/ResyncIdleCarriers", req, opts...)
}

// --- NodeArenaP2PService (kontinued <-> kontinued peer chunk transfer) ---

type GetManifestRequest struct {
	SnapshotID string `json:"snapshotId"`
}

type GetManifestResponse struct {
	Manifest SnapshotManifest `json:"manifest"`
}

type GetChunkRequest struct {
	Blake3Hash string `json:"blake3Hash"`
}

type GetChunkResponse struct {
	Blake3Hash string `json:"blake3Hash"`
	Data       []byte `json:"data"`
}

type StreamChunksRequest struct {
	Blake3Hashes []string `json:"blake3Hashes"`
}

type ChunkFrame struct {
	Blake3Hash string `json:"blake3Hash"`
	Data       []byte `json:"data"`
}

type NodeArenaP2PServer interface {
	GetManifest(context.Context, *GetManifestRequest) (*GetManifestResponse, error)
	GetChunk(context.Context, *GetChunkRequest) (*GetChunkResponse, error)
	StreamChunks(*StreamChunksRequest, NodeArenaStreamServer) error
}

type NodeArenaStreamServer interface {
	Send(*ChunkFrame) error
	grpc.ServerStream
}

type nodeArenaStreamServer struct{ grpc.ServerStream }

func (s *nodeArenaStreamServer) Send(m *ChunkFrame) error { return s.ServerStream.SendMsg(m) }

var NodeArenaP2PServiceDesc = grpc.ServiceDesc{
	ServiceName: "kontinue.p2p.v1.NodeArenaP2PService",
	HandlerType: (*NodeArenaP2PServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetManifest",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(GetManifestRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(NodeArenaP2PServer).GetManifest(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/kontinue.p2p.v1.NodeArenaP2PService/GetManifest"}
				return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
					return srv.(NodeArenaP2PServer).GetManifest(ctx, req.(*GetManifestRequest))
				})
			},
		},
		{
			MethodName: "GetChunk",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(GetChunkRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(NodeArenaP2PServer).GetChunk(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/kontinue.p2p.v1.NodeArenaP2PService/GetChunk"}
				return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
					return srv.(NodeArenaP2PServer).GetChunk(ctx, req.(*GetChunkRequest))
				})
			},
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName: "StreamChunks",
			Handler: func(srv any, stream grpc.ServerStream) error {
				in := new(StreamChunksRequest)
				if err := stream.RecvMsg(in); err != nil {
					return err
				}
				return srv.(NodeArenaP2PServer).StreamChunks(in, &nodeArenaStreamServer{stream})
			},
			ServerStreams: true,
		},
	},
	Metadata: "kontinue/p2p.json",
}

func RegisterNodeArenaP2PServer(s grpc.ServiceRegistrar, srv NodeArenaP2PServer) {
	s.RegisterService(&NodeArenaP2PServiceDesc, srv)
}

type NodeArenaP2PClient struct {
	cc grpc.ClientConnInterface
}

func NewNodeArenaP2PClient(cc grpc.ClientConnInterface) *NodeArenaP2PClient {
	return &NodeArenaP2PClient{cc: cc}
}

func (c *NodeArenaP2PClient) GetManifest(ctx context.Context, req *GetManifestRequest, opts ...grpc.CallOption) (*GetManifestResponse, error) {
	return invokeJSON[GetManifestRequest, GetManifestResponse](ctx, c.cc, "/kontinue.p2p.v1.NodeArenaP2PService/GetManifest", req, opts...)
}

func (c *NodeArenaP2PClient) GetChunk(ctx context.Context, req *GetChunkRequest, opts ...grpc.CallOption) (*GetChunkResponse, error) {
	return invokeJSON[GetChunkRequest, GetChunkResponse](ctx, c.cc, "/kontinue.p2p.v1.NodeArenaP2PService/GetChunk", req, opts...)
}

type StreamChunksClient struct{ grpc.ClientStream }

func (x *StreamChunksClient) Recv() (*ChunkFrame, error) {
	m := new(ChunkFrame)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *NodeArenaP2PClient) StreamChunks(ctx context.Context, req *StreamChunksRequest, opts ...grpc.CallOption) (*StreamChunksClient, error) {
	opts = append([]grpc.CallOption{grpc.ForceCodec(jsonCodec{})}, opts...)
	cs, err := c.cc.NewStream(ctx, &NodeArenaP2PServiceDesc.Streams[0], "/kontinue.p2p.v1.NodeArenaP2PService/StreamChunks", opts...)
	if err != nil {
		return nil, err
	}
	x := &StreamChunksClient{cs}
	if err := x.SendMsg(req); err != nil {
		return nil, err
	}
	if err := x.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}
