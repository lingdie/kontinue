package protocol

import "time"

const (
	// DefaultChunkSize is 2 MiB, aligned with Linux Transparent Hugepages (PMD) and gVisor MemoryFile blocks.
	DefaultChunkSize = 2 * 1024 * 1024

	GVisorCheckpointImg = "checkpoint.img"
	GVisorPagesMetaImg  = "pages_meta.img"
	GVisorPagesImg      = "pages.img"
	RootfsUpperTar      = "rootfs-upper.tar"
	ManifestFileName    = "kontinue-manifest.json"
	HotPagesFileName    = "hot-pages.idx"
)

type SnapshotKind string

const (
	SnapshotKindGoldenTemplate SnapshotKind = "GoldenTemplate"
	SnapshotKindSessionSuspend SnapshotKind = "SessionSuspend"
	SnapshotKindLiveFork       SnapshotKind = "LiveFork"
)

// CompatibilityFingerprint captures node CPU and runtime attributes required for safe memory restore.
// Note: GPUDriverVersion is reserved as an optional field for future post-MVP cuda-checkpoint extensions.
type CompatibilityFingerprint struct {
	Architecture     string   `json:"architecture"`
	CPUProfileHash   string   `json:"cpuProfileHash"`
	CPUFeatures      []string `json:"cpuFeatures,omitempty"`
	RunscVersion     string   `json:"runscVersion"`
	KernelRelease    string   `json:"kernelRelease,omitempty"`
	Arm64NoPauth     bool     `json:"arm64NoPauth,omitempty"`
	GPUDriverVersion string   `json:"gpuDriverVersion,omitempty"`
}

// CompatibleWith verifies whether a target worker node can safely memory-restore a snapshot with fp.
func (fp CompatibilityFingerprint) CompatibleWith(node CompatibilityFingerprint) bool {
	if fp.Architecture != "" && node.Architecture != "" && fp.Architecture != node.Architecture {
		return false
	}
	if fp.CPUProfileHash != "" && node.CPUProfileHash != "" && fp.CPUProfileHash != node.CPUProfileHash {
		return false
	}
	if fp.RunscVersion != "" && node.RunscVersion != "" && fp.RunscVersion != node.RunscVersion {
		return false
	}
	if node.Architecture == "arm64" && !node.Arm64NoPauth && fp.Arm64NoPauth {
		return false
	}
	return true
}

// ChunkRef maps a 2 MiB slice of pages.img to a content-addressed Blake3 chunk in CAS or a parent snapshot layer.
type ChunkRef struct {
	Index            uint32 `json:"index"`
	Offset           int64  `json:"offset"`
	Length           int32  `json:"length"`
	Blake3Hash       string `json:"blake3Hash,omitempty"`
	ZeroChunk        bool   `json:"zeroChunk,omitempty"`
	ReusedFromParent bool   `json:"reusedFromParent,omitempty"`
}

// SnapshotManifest is shared identically between Go services and Rust kontinue-arena-helper.
type SnapshotManifest struct {
	SchemaVersion      int                      `json:"schemaVersion"`
	SnapshotID         string                   `json:"snapshotId"`
	Kind               SnapshotKind             `json:"kind"`
	TemplateID         string                   `json:"templateId,omitempty"`
	BaseImage          string                   `json:"baseImage"`
	ParentSnapshotID   string                   `json:"parentSnapshotId,omitempty"`
	CreatedAt          time.Time                `json:"createdAt"`
	IncludeMemory      bool                     `json:"includeMemory"`
	ChunkSize          int32                    `json:"chunkSize"`
	CheckpointImgHash  string                   `json:"checkpointImgHash,omitempty"`
	CheckpointImgSize  int64                    `json:"checkpointImgSize,omitempty"`
	PagesMetaImgHash   string                   `json:"pagesMetaImgHash,omitempty"`
	PagesMetaImgSize   int64                    `json:"pagesMetaImgSize,omitempty"`
	PagesImgTotalSize  int64                    `json:"pagesImgTotalSize"`
	DirtyMemoryBytes   int64                    `json:"dirtyMemoryBytes"`
	DirtyChunkCount    int32                    `json:"dirtyChunkCount"`
	ReusedChunkCount   int32                    `json:"reusedChunkCount"`
	ZeroChunkCount     int32                    `json:"zeroChunkCount"`
	RootfsUpperTarHash string                   `json:"rootfsUpperTarHash,omitempty"`
	RootfsUpperTarSize int64                    `json:"rootfsUpperTarSize,omitempty"`
	Chunks             []ChunkRef               `json:"chunks,omitempty"`
	HotPageIndices     []uint32                 `json:"hotPageIndices,omitempty"`
	OriginNodeID       string                   `json:"originNodeId"`
	OriginPeerAddr     string                   `json:"originPeerAddr,omitempty"`
	BootstrapToken     string                   `json:"bootstrapToken,omitempty"`
	Fingerprint        CompatibilityFingerprint `json:"fingerprint"`
}

// KubeletCheckpointDescriptor is written inside /var/lib/kubelet/checkpoints/checkpoint-*.tar
// to satisfy Kubelet's native POST /checkpoint/{ns}/{pod}/{container} API without copying multi-GB RAM files twice.
type KubeletCheckpointDescriptor struct {
	SnapshotID     string    `json:"snapshotId"`
	ArenaLocalPath string    `json:"arenaLocalPath"`
	ManifestURI    string    `json:"manifestUri,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	TotalBytes     int64     `json:"totalBytes"`
	DirtyBytes     int64     `json:"dirtyBytes"`
}
