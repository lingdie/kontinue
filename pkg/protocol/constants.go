package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func AccessDigest(token string) string {
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

const (
	// RuntimeClassName is the Kubernetes RuntimeClass backed by kontinue-runsc.
	RuntimeClassName = "kontinue-gvisor"

	// DefaultKtdPort is the control and execution port inside every sandbox container.
	DefaultKtdPort = 49983

	// Container / Pod annotations and labels (kontinue.sh/*).
	AnnotationRestoreSnapshot = "kontinue.sh/restore-snapshot"
	AnnotationRestoreMode     = "kontinue.sh/restore-mode"
	AnnotationSandboxID       = "kontinue.sh/sandbox-id"
	AnnotationTemplateID      = "kontinue.sh/template-id"
	AnnotationExecutionID     = "kontinue.sh/execution-id"
	AnnotationLifecycleID     = "kontinue.sh/lifecycle-id"
	AnnotationCarrierTier     = "kontinue.sh/carrier-tier"
	AnnotationTraceParent     = "kontinue.sh/traceparent"

	// Upstream gVisor containerd-shim-runsc-v1 restore control annotations.
	GVisorAnnotationRestoreHostPath = "dev.gvisor.internal.restore.host-image-path"
	GVisorAnnotationRestoreDirect   = "dev.gvisor.internal.restore.direct"

	// Kubernetes labels for Carrier Pods and Node pools.
	LabelRole            = "kontinue.sh/role"
	LabelCarrierTier     = "kontinue.sh/tier"
	LabelColdLifecycle   = "kontinue.sh/cold-lifecycle"
	LabelColdNetworkOnly = "kontinue.sh/cold-network-only"
	LabelCPUProfile      = "kontinue.sh/cpu-profile"
	LabelMemoryCkptReady = "kontinue.sh/memory-ckpt-ready"

	RoleCarrierPod    = "carrier-pod"
	RoleWarmupBuilder = "template-builder"

	DefaultCarrierTier    = "default"
	ContainerNameSandbox  = "sandbox"
	K8sLabelPodName       = "io.kubernetes.pod.name"
	K8sLabelPodNamespace  = "io.kubernetes.pod.namespace"
	K8sLabelPodUID        = "io.kubernetes.pod.uid"
	K8sLabelContainerName = "io.kubernetes.container.name"
	K8sLabelContainerHash = "io.kubernetes.container.hash"

	// Restore modes.
	RestoreModeMemory         = "memory"
	RestoreModeFilesystemOnly = "filesystem-only"
	RestoreModeCold           = "cold"

	// HTTP Headers across Gateway, API Server, Node Daemon, and Guest Agent (ktd).
	HeaderKontinueToken       = "X-Kontinue-Token"
	HeaderBootstrapToken      = "X-Bootstrap-Token"
	HeaderKontinueSandboxID   = "X-Kontinue-Sandbox-ID"
	HeaderKontinueExecutionID = "X-Kontinue-Execution-ID"
	HeaderIdempotencyKey      = "Idempotency-Key"
	HeaderTraceID             = "X-Trace-ID"

	// Guest Agent (ktd) internal control-plane endpoints (blocked by kontinue-gateway).
	PathInternalHealth = "/internal/health"
	PathInternalFreeze = "/internal/freeze"
	PathInternalThaw   = "/internal/thaw"
	PathInternalInit   = "/internal/init"

	// Guest Agent (ktd) public SDK endpoints.
	PathProcessExec      = "/v1/process/exec"
	PathProcessList      = "/v1/process/list"
	PathProcessKill      = "/v1/process/kill"
	PathProcessLogs      = "/v1/process/logs"
	PathProcessLogStream = "/v1/process/logs/stream"
	PathPTYSession       = "/v1/pty"
	PathFiles            = "/v1/files"
	PathFilesStream      = "/v1/files/stream"
	PathFilesWatch       = "/v1/files/watch"
	PathFilesInfo        = "/v1/files/info"
	PathFilesList        = "/v1/files/list"
	PathFilesMkdir       = "/v1/files/mkdir"
	PathFilesRename      = "/v1/files/rename"

	// Self-diagnosis flight recorder endpoint exposed on every service.
	PathDebugTraces = "/debug/traces"

	// Redis Key Prefixes (kt:*).
	RedisPrefixRoute       = "kt:route:"
	RedisPrefixLock        = "kt:lock:"
	RedisPrefixActiveAt    = "kt:active_at:"
	RedisPrefixNodeStatus  = "kt:node:status:"
	RedisPrefixNodeSet     = "kt:nodes:active"
	RedisPrefixIdlePool    = "kt:pool:idle:"
	RedisPrefixCarrierMeta = "kt:carrier:meta:"
	RedisPrefixIdempotency = "kt:idempotency:"
	RedisPrefixInflight    = "kt:inflight:"
)

// Background leader lock names (hashed into PostgreSQL advisory locks).
const (
	LeaderReplenisher = "kontinue.replenisher"
	LeaderEvictor     = "kontinue.evictor"
)

// IsInternalControlPath returns true if an HTTP path targets node-only ktd endpoints.
func IsInternalControlPath(p string) bool {
	return p == "/internal" || strings.HasPrefix(p, "/internal/")
}

func RouteKey(sandboxID string) string       { return RedisPrefixRoute + sandboxID }
func LockKey(sandboxID string) string        { return RedisPrefixLock + sandboxID }
func ActiveAtKey(sandboxID string) string    { return RedisPrefixActiveAt + sandboxID }
func NodeStatusKey(nodeID string) string     { return RedisPrefixNodeStatus + nodeID }
func IdlePoolKey(nodeID, tier string) string { return RedisPrefixIdlePool + nodeID + ":" + tier }
func CarrierMetaKey(slotID string) string    { return RedisPrefixCarrierMeta + slotID }
func InflightKey(sandboxID string) string    { return RedisPrefixInflight + sandboxID }
