package store

import (
	"context"
	"errors"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

var (
	ErrNotFound            = errors.New("record not found")
	ErrStaleExecutionFence = errors.New("stale executionId rejected by route fence")
	ErrNoIdleCarrierSlot   = errors.New("no pre-warmed carrier pod slot available")
	ErrTransitionLocked    = errors.New("sandbox state transition already in progress")
)

const AdmissionTTL = 2 * time.Minute

type SandboxState string

const (
	SandboxStateCreating   SandboxState = "CREATING"
	SandboxStateRunning    SandboxState = "RUNNING"
	SandboxStateSuspending SandboxState = "SUSPENDING"
	SandboxStateSuspended  SandboxState = "SUSPENDED"
	SandboxStateResuming   SandboxState = "RESUMING"
	SandboxStateTerminated SandboxState = "TERMINATED"
	SandboxStateFailed     SandboxState = "FAILED"
)

type TemplateRecord struct {
	TemplateID       string    `json:"templateId"`
	Name             string    `json:"name"`
	BaseImage        string    `json:"baseImage"`
	WarmupScript     string    `json:"warmupScript,omitempty"`
	Tier             string    `json:"tier"`
	PinInL1Tmpfs     bool      `json:"pinInL1Tmpfs"`
	TraceHotPages    bool      `json:"traceHotPages"`
	Phase            string    `json:"phase"`
	GoldenSnapshotID string    `json:"goldenSnapshotId,omitempty"`
	StatusMessage    string    `json:"statusMessage,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type SnapshotRecord struct {
	SnapshotID       string                            `json:"snapshotId"`
	Kind             protocol.SnapshotKind             `json:"kind"`
	TemplateID       string                            `json:"templateId,omitempty"`
	SourceSandboxID  string                            `json:"sourceSandboxId,omitempty"`
	ParentSnapshotID string                            `json:"parentSnapshotId,omitempty"`
	BaseImage        string                            `json:"baseImage"`
	ManifestURI      string                            `json:"manifestUri"`
	OriginNodeID     string                            `json:"originNodeId"`
	OriginPeerAddr   string                            `json:"originPeerAddr,omitempty"`
	TotalMemoryBytes int64                             `json:"totalMemoryBytes"`
	DirtyMemoryBytes int64                             `json:"dirtyMemoryBytes"`
	DirtyChunks      int32                             `json:"dirtyChunks"`
	ReusedChunks     int32                             `json:"reusedChunks"`
	Superseded       bool                              `json:"superseded"`
	BootstrapToken   string                            `json:"bootstrapToken,omitempty"`
	Fingerprint      protocol.CompatibilityFingerprint `json:"fingerprint"`
	CreatedAt        time.Time                         `json:"createdAt"`
}

type SandboxRecord struct {
	SandboxID                   string            `json:"sandboxId"`
	TemplateID                  string            `json:"templateId,omitempty"`
	BaseImage                   string            `json:"baseImage"`
	Tier                        string            `json:"tier"`
	State                       SandboxState      `json:"state"`
	ExecutionID                 int64             `json:"executionId"`
	LifecycleID                 string            `json:"lifecycleId"`
	PendingSnapshotID           string            `json:"pendingSnapshotId,omitempty"`
	ActiveSnapshotID            string            `json:"activeSnapshotId,omitempty"`
	CurrentNodeID               string            `json:"currentNodeId,omitempty"`
	OriginNodeID                string            `json:"originNodeId,omitempty"`
	CurrentSlotID               string            `json:"currentSlotId,omitempty"`
	CarrierPodNamespace         string            `json:"carrierPodNamespace,omitempty"`
	CarrierPodName              string            `json:"carrierPodName,omitempty"`
	PodIP                       string            `json:"podIp,omitempty"`
	PoolAllocation              string            `json:"poolAllocation,omitempty"`
	AccessToken                 string            `json:"accessToken,omitempty"`
	BootstrapToken              string            `json:"bootstrapToken,omitempty"`
	EnvVars                     map[string]string `json:"envVars,omitempty"`
	IdleTimeoutSeconds          int64             `json:"idleTimeoutSeconds"`
	CPUMillis                   int64             `json:"cpuMillis,omitempty"`
	MemoryBytes                 int64             `json:"memoryBytes,omitempty"`
	AllowFilesystemOnlyFallback bool              `json:"allowFilesystemOnlyFallback"`
	CreatedAt                   time.Time         `json:"createdAt"`
	UpdatedAt                   time.Time         `json:"updatedAt"`
}

// MetadataStore persists durable templates, snapshots, and sandbox lifecycles (PostgreSQL).
type MetadataStore interface {
	Migrate(ctx context.Context) error

	CreateTemplate(ctx context.Context, tpl *TemplateRecord) error
	GetTemplate(ctx context.Context, templateID string) (*TemplateRecord, error)
	UpdateTemplateStatus(ctx context.Context, templateID, phase, goldenSnapshotID, msg string) error
	ListTemplates(ctx context.Context) ([]TemplateRecord, error)

	CreateSnapshot(ctx context.Context, snap *SnapshotRecord) error
	GetSnapshot(ctx context.Context, snapshotID string) (*SnapshotRecord, error)
	MarkSnapshotSuperseded(ctx context.Context, snapshotID string) error
	ListGCandidateSnapshots(ctx context.Context) ([]SnapshotRecord, error)
	ListAllSnapshots(ctx context.Context) ([]SnapshotRecord, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error

	CreateSandbox(ctx context.Context, sbx *SandboxRecord) error
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error)
	SaveSandbox(ctx context.Context, sbx *SandboxRecord) error
	ListSandboxesToSweep(ctx context.Context) ([]SandboxRecord, error)
	DeleteSandbox(ctx context.Context, sandboxID string) error

	// TryLeader acquires a cluster-wide leader lock for background workers.
	// release must be called when the critical section ends. ok=false means another replica holds it.
	TryLeader(ctx context.Context, name string) (release func(), ok bool, err error)
}

// RuntimeCatalog coordinates sub-millisecond routes, Carrier Pod slots, ExecutionID fences, and node heartbeats (Redis).
type RuntimeCatalog interface {
	RegisterIdleSlot(ctx context.Context, slot protocol.CarrierSlot) error
	RemoveIdleSlot(ctx context.Context, nodeID, tier, slotID string) error
	ClaimIdleSlot(ctx context.Context, nodeID, tier string) (*protocol.CarrierSlot, error)
	CountIdleSlots(ctx context.Context, nodeID, tier string) (int, error)

	PublishRoute(ctx context.Context, route protocol.RouteRecord) error
	GetRoute(ctx context.Context, sandboxID string) (*protocol.RouteRecord, error)
	DeleteRouteFenced(ctx context.Context, sandboxID string, executionID int64, slotID string) error

	AdmitRoute(ctx context.Context, sandboxID, requestID string) (*protocol.RouteRecord, error)
	RenewAdmission(ctx context.Context, sandboxID, requestID string) error
	FinishAdmission(ctx context.Context, sandboxID, requestID string) error
	DrainRoute(ctx context.Context, route protocol.RouteRecord) error

	TouchSandboxActive(ctx context.Context, sandboxID string, at time.Time) error
	GetLastActive(ctx context.Context, sandboxID string) (time.Time, bool, error)

	// AdjustInflight tracks gateway in-flight connections per sandbox (positive delta increments).
	AdjustInflight(ctx context.Context, sandboxID string, delta int64) (int64, error)
	Inflight(ctx context.Context, sandboxID string) (int64, error)

	UpsertNodeHeartbeat(ctx context.Context, hb protocol.NodeHeartbeat, ttl time.Duration) error
	ListHealthyNodes(ctx context.Context) ([]protocol.NodeHeartbeat, error)

	SaveIdempotency(ctx context.Context, key string, statusCode int, body []byte, ttl time.Duration) error
	GetIdempotency(ctx context.Context, key string) (statusCode int, body []byte, ok bool, err error)
}
