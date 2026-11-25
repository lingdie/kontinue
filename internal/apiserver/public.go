package apiserver

import (
	"maps"
	"slices"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

// Public response types list the fields the HTTP API can disclose. Node and
// store records also contain credentials and partially completed operations.
type sandboxResponse struct {
	ArenaTier                   string                 `json:"arenaTier,omitempty"`
	RestoreViewCopiedBytes      *int64                 `json:"restoreViewCopiedBytes,omitempty"`
	SandboxID                   string                 `json:"sandboxId"`
	TemplateID                  string                 `json:"templateId,omitempty"`
	BaseImage                   string                 `json:"baseImage"`
	Tier                        string                 `json:"tier"`
	State                       store.SandboxState     `json:"state"`
	ExecutionID                 int64                  `json:"executionId"`
	LifecycleID                 string                 `json:"lifecycleId"`
	ActiveSnapshotID            string                 `json:"activeSnapshotId,omitempty"`
	CurrentNodeID               string                 `json:"currentNodeId,omitempty"`
	OriginNodeID                string                 `json:"originNodeId,omitempty"`
	CurrentSlotID               string                 `json:"currentSlotId,omitempty"`
	CarrierPodNamespace         string                 `json:"carrierPodNamespace,omitempty"`
	CarrierPodName              string                 `json:"carrierPodName,omitempty"`
	PodIP                       string                 `json:"podIp,omitempty"`
	PoolAllocation              string                 `json:"poolAllocation,omitempty"`
	IdleTimeoutSeconds          int64                  `json:"idleTimeoutSeconds"`
	CPUMillis                   int64                  `json:"cpuMillis,omitempty"`
	MemoryBytes                 int64                  `json:"memoryBytes,omitempty"`
	AllowFilesystemOnlyFallback bool                   `json:"allowFilesystemOnlyFallback"`
	Metadata                    map[string]string      `json:"metadata,omitempty"`
	Volumes                     []protocol.VolumeMount `json:"volumes,omitempty"`
	FailureCode                 string                 `json:"failureCode,omitempty"`
	FailureReason               string                 `json:"failureReason,omitempty"`
	CreatedAt                   time.Time              `json:"createdAt"`
	UpdatedAt                   time.Time              `json:"updatedAt"`
}

func publicSandbox(r *store.SandboxRecord) sandboxResponse {
	response := sandboxResponse{ArenaTier: r.ArenaTier, RestoreViewCopiedBytes: r.RestoreViewCopiedBytes,
		SandboxID:                   r.SandboxID,
		TemplateID:                  r.TemplateID,
		BaseImage:                   r.BaseImage,
		Tier:                        r.Tier,
		State:                       r.State,
		ExecutionID:                 r.ExecutionID,
		LifecycleID:                 r.LifecycleID,
		ActiveSnapshotID:            r.ActiveSnapshotID,
		CurrentNodeID:               r.CurrentNodeID,
		OriginNodeID:                r.OriginNodeID,
		CurrentSlotID:               r.CurrentSlotID,
		CarrierPodNamespace:         r.CarrierPodNamespace,
		CarrierPodName:              r.CarrierPodName,
		PodIP:                       r.PodIP,
		PoolAllocation:              r.PoolAllocation,
		IdleTimeoutSeconds:          r.IdleTimeoutSeconds,
		CPUMillis:                   r.CPUMillis,
		MemoryBytes:                 r.MemoryBytes,
		AllowFilesystemOnlyFallback: r.AllowFilesystemOnlyFallback,
		Metadata:                    maps.Clone(r.Metadata),
		Volumes:                     slices.Clone(r.Volumes),
		CreatedAt:                   r.CreatedAt,
		UpdatedAt:                   r.UpdatedAt,
	}
	if r.State == store.SandboxStateFailed || r.FailureReason != "" {
		response.FailureCode = "sandbox_failed"
		response.FailureReason = "sandbox execution failed"
	}
	return response
}

type snapshotResponse struct {
	SnapshotID       string                            `json:"snapshotId"`
	Kind             protocol.SnapshotKind             `json:"kind"`
	TemplateID       string                            `json:"templateId,omitempty"`
	SourceSandboxID  string                            `json:"sourceSandboxId,omitempty"`
	ParentSnapshotID string                            `json:"parentSnapshotId,omitempty"`
	BaseImage        string                            `json:"baseImage"`
	OriginNodeID     string                            `json:"originNodeId"`
	TotalMemoryBytes int64                             `json:"totalMemoryBytes"`
	DirtyMemoryBytes int64                             `json:"dirtyMemoryBytes"`
	DirtyChunks      int32                             `json:"dirtyChunks"`
	ReusedChunks     int32                             `json:"reusedChunks"`
	Superseded       bool                              `json:"superseded"`
	Fingerprint      protocol.CompatibilityFingerprint `json:"fingerprint"`
	CreatedAt        time.Time                         `json:"createdAt"`
}

func publicSnapshot(r *store.SnapshotRecord) snapshotResponse {
	return snapshotResponse{
		SnapshotID:       r.SnapshotID,
		Kind:             r.Kind,
		TemplateID:       r.TemplateID,
		SourceSandboxID:  r.SourceSandboxID,
		ParentSnapshotID: r.ParentSnapshotID,
		BaseImage:        r.BaseImage,
		OriginNodeID:     r.OriginNodeID,
		TotalMemoryBytes: r.TotalMemoryBytes,
		DirtyMemoryBytes: r.DirtyMemoryBytes,
		DirtyChunks:      r.DirtyChunks,
		ReusedChunks:     r.ReusedChunks,
		Superseded:       r.Superseded,
		Fingerprint:      r.Fingerprint,
		CreatedAt:        r.CreatedAt,
	}
}

type templateResponse struct {
	TemplateID       string            `json:"templateId"`
	Name             string            `json:"name"`
	BaseImage        string            `json:"baseImage"`
	Tier             string            `json:"tier"`
	Version          int               `json:"version"`
	Tags             map[string]string `json:"tags,omitempty"`
	PinInL1Tmpfs     bool              `json:"pinInL1Tmpfs"`
	TraceHotPages    bool              `json:"traceHotPages"`
	Phase            string            `json:"phase"`
	GoldenSnapshotID string            `json:"goldenSnapshotId,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

func publicTemplate(r *store.TemplateRecord) templateResponse {
	return templateResponse{
		TemplateID:       r.TemplateID,
		Name:             r.Name,
		BaseImage:        r.BaseImage,
		Tier:             r.Tier,
		Version:          r.Version,
		Tags:             maps.Clone(r.Tags),
		PinInL1Tmpfs:     r.PinInL1Tmpfs,
		TraceHotPages:    r.TraceHotPages,
		Phase:            r.Phase,
		GoldenSnapshotID: r.GoldenSnapshotID,
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
	}
}

type templateBuildResponse struct {
	Version       int       `json:"version"`
	SnapshotID    string    `json:"snapshotId,omitempty"`
	Phase         string    `json:"phase"`
	StatusMessage string    `json:"statusMessage,omitempty"`
	Logs          []string  `json:"logs,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	CompletedAt   time.Time `json:"completedAt,omitempty"`
}

func publicTemplateBuild(r store.TemplateBuildRecord) templateBuildResponse {
	return templateBuildResponse{
		Version: r.Version, SnapshotID: r.SnapshotID, Phase: r.Phase, StatusMessage: r.StatusMessage,
		Logs: slices.Clone(r.Logs), CreatedAt: r.CreatedAt, CompletedAt: r.CompletedAt,
	}
}

type issuedSandbox struct {
	sandboxResponse
	AccessToken string `json:"accessToken"`
}

type forkChildResponse struct {
	ChildID string         `json:"childId"`
	Sandbox *issuedSandbox `json:"sandbox,omitempty"`
	Error   string         `json:"error,omitempty"`
}

func issueSandbox(r *store.SandboxRecord) issuedSandbox {
	return issuedSandbox{sandboxResponse: publicSandbox(r), AccessToken: r.AccessToken}
}
