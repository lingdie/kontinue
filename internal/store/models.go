package store

import (
	"context"
	"errors"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

var (
	ErrSandboxExists       = errors.New("sandbox ID is already in use")
	ErrSnapshotExists      = errors.New("snapshot ID is already in use")
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
	SandboxStateDeleting   SandboxState = "DELETING"
	SandboxStateTerminated SandboxState = "TERMINATED"
	SandboxStateFailed     SandboxState = "FAILED"
)

const TemplateCapturing = "Capturing"

type TemplateBuild struct {
	SnapshotID     string               `json:"snapshotId"`
	LifecycleID    string               `json:"lifecycleId"`
	BootstrapToken string               `json:"bootstrapToken"`
	AccessToken    string               `json:"accessToken"`
	Slot           protocol.CarrierSlot `json:"slot"`
	PeerAddr       string               `json:"peerAddr,omitempty"`
	GuestEndpoint  string               `json:"guestEndpoint,omitempty"`
}

type TemplateRecord struct {
	TenantID         string         `json:"-"`
	Build            *TemplateBuild `json:"-"`
	TemplateID       string         `json:"templateId"`
	Name             string         `json:"name"`
	BaseImage        string         `json:"baseImage"`
	WarmupScript     string         `json:"warmupScript,omitempty"`
	Tier             string         `json:"tier"`
	PinInL1Tmpfs     bool           `json:"pinInL1Tmpfs"`
	TraceHotPages    bool           `json:"traceHotPages"`
	Phase            string         `json:"phase"`
	GoldenSnapshotID string         `json:"goldenSnapshotId,omitempty"`
	StatusMessage    string         `json:"statusMessage,omitempty"`
	CreatedAt        time.Time      `json:"createdAt"`
	UpdatedAt        time.Time      `json:"updatedAt"`
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

type ActivationPlacement struct {
	Slot             protocol.CarrierSlot `json:"slot"`
	RestoreMode      string               `json:"restoreMode"`
	HydrationStarted bool                 `json:"hydrationStarted,omitempty"`
}

// ForkReceipt identifies the branches even after a lost HTTP response.
type ForkReceipt struct {
	OperationID string   `json:"operationId"`
	RequestHash string   `json:"requestHash"`
	ChildIDs    []string `json:"childIds"`
}
type PendingFork struct {
	ForkReceipt
	RequestKey string            `json:"requestKey,omitempty"`
	SnapshotID string            `json:"snapshotId"`
	EnvVars    map[string]string `json:"envVars,omitempty"`
}

type SandboxRecord struct {
	ArenaTier                   string               `json:"arenaTier,omitempty"`
	RestoreViewCopiedBytes      *int64               `json:"restoreViewCopiedBytes,omitempty"`
	CreateKey                   string               `json:"createKey,omitempty"`
	CreateRequestHash           string               `json:"createRequestHash,omitempty"`
	TenantID                    string               `json:"tenantId,omitempty"`
	PendingFork                 *PendingFork         `json:"pendingFork,omitempty"`
	ForkOperationID             string               `json:"forkOperationId,omitempty"`
	PendingPlacement            *ActivationPlacement `json:"pendingPlacement,omitempty"`
	FailureReason               string               `json:"failureReason,omitempty"`
	SandboxID                   string               `json:"sandboxId"`
	TemplateID                  string               `json:"templateId,omitempty"`
	BaseImage                   string               `json:"baseImage"`
	Tier                        string               `json:"tier"`
	State                       SandboxState         `json:"state"`
	ExecutionID                 int64                `json:"executionId"`
	LifecycleID                 string               `json:"lifecycleId"`
	PendingSnapshotID           string               `json:"pendingSnapshotId,omitempty"`
	ActiveSnapshotID            string               `json:"activeSnapshotId,omitempty"`
	CurrentNodeID               string               `json:"currentNodeId,omitempty"`
	OriginNodeID                string               `json:"originNodeId,omitempty"`
	CurrentSlotID               string               `json:"currentSlotId,omitempty"`
	CarrierPodNamespace         string               `json:"carrierPodNamespace,omitempty"`
	CarrierPodName              string               `json:"carrierPodName,omitempty"`
	PodIP                       string               `json:"podIp,omitempty"`
	PoolAllocation              string               `json:"poolAllocation,omitempty"`
	AccessToken                 string               `json:"accessToken,omitempty"`
	BootstrapToken              string               `json:"bootstrapToken,omitempty"`
	EnvVars                     map[string]string    `json:"envVars,omitempty"`
	IdleTimeoutSeconds          int64                `json:"idleTimeoutSeconds"`
	CPUMillis                   int64                `json:"cpuMillis,omitempty"`
	MemoryBytes                 int64                `json:"memoryBytes,omitempty"`
	AllowFilesystemOnlyFallback bool                 `json:"allowFilesystemOnlyFallback"`
	CreatedAt                   time.Time            `json:"createdAt"`
	UpdatedAt                   time.Time            `json:"updatedAt"`
}

// MetadataStore persists durable templates, snapshots, and sandbox lifecycles (PostgreSQL).
type MetadataStore interface {
	Migrate(ctx context.Context) error

	CreateTemplate(ctx context.Context, tpl *TemplateRecord) error
	GetTemplate(ctx context.Context, templateID string) (*TemplateRecord, error)
	SaveTemplateBuild(ctx context.Context, tpl *TemplateRecord) error
	ListPendingTemplateIDs(ctx context.Context) ([]string, error)
	ListTemplates(ctx context.Context) ([]TemplateRecord, error)

	// LockSnapshotSet lets publications overlap and excludes physical collection.
	LockSnapshotSet(ctx context.Context, exclusive bool) (release func(), err error)
	CreateSnapshot(ctx context.Context, snap *SnapshotRecord) error
	GetSnapshot(ctx context.Context, snapshotID string) (*SnapshotRecord, error)
	MarkSnapshotSuperseded(ctx context.Context, snapshotID string) error
	ListGCandidateSnapshots(ctx context.Context) ([]SnapshotRecord, error)
	ListAllSnapshots(ctx context.Context) ([]SnapshotRecord, error)
	UnpublishedSnapshot(ctx context.Context) (string, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error

	CreateSandbox(ctx context.Context, sbx *SandboxRecord) error
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error)
	CommitFork(ctx context.Context, source *SandboxRecord, children []*SandboxRecord) error
	GetForkReceipt(ctx context.Context, sourceID, requestKey string) (*ForkReceipt, error)
	SaveSandbox(ctx context.Context, sbx *SandboxRecord) error
	ListSandboxesToSweep(ctx context.Context) ([]SandboxRecord, error)
	DeleteSandbox(ctx context.Context, sandboxID string) error

	// TryLeader acquires a cluster-wide leader lock for background workers.
	// release must be called when the critical section ends. ok=false means another replica holds it.
	TryLeader(ctx context.Context, name string) (release func(), ok bool, err error)
}

// RuntimeCatalog coordinates sub-millisecond routes, Carrier Pod slots, ExecutionID fences, and node heartbeats (Redis).
type SlotReservation struct {
	Slot protocol.CarrierSlot `json:"slot"`
	Cold bool                 `json:"cold"`
}

type RuntimeCatalog interface {
	ReserveIdleSlot(ctx context.Context, nodeID, tier, lifecycle string, cold bool) (*SlotReservation, error)
	ExpectColdCarrier(ctx context.Context, lifecycle, slotID string, ttl time.Duration) error
	ExpectedColdCarrier(ctx context.Context, lifecycle string) (string, error)
	FinishReservation(ctx context.Context, lifecycle string, returnUnused bool) error
	BeginNodeRecovery(context.Context, string) error
	ReportExecution(context.Context, protocol.RouteRecord, ExecutionReport) error
	ExecutionReport(context.Context, protocol.RouteRecord) (ExecutionReport, error)
	RegisterIdleSlot(ctx context.Context, slot protocol.CarrierSlot) error
	RegisterColdCarrier(ctx context.Context, slot protocol.CarrierSlot, lifecycle string) (published bool, err error)
	RemoveIdleSlot(ctx context.Context, slot protocol.CarrierSlot) error
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
