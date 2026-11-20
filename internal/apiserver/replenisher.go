package apiserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type ReplenisherConfig struct {
	Namespace        string
	CarrierImage     string
	RuntimeClassName string
	// Leader lets only one apiserver replica top up the pool.
	Leader         store.MetadataStore
	MinIdlePerNode int
	// IdleBurstBuffer is extra idle capacity above MinIdlePerNode used as the
	// asynchronous top-up target (TargetIdle = MinIdle + IdleBurstBuffer).
	IdleBurstBuffer int
	// ColdBurstExtra creates this many additional Carrier Pods on each cold
	// allocate so concurrent siblings can claim surplus warm slots.
	ColdBurstExtra int
	PollInterval   time.Duration
	// ResyncNode optionally triggers an immediate CarrierPodWatcher sync on the
	// target node after cold Pod creates (avoids waiting for the poll ticker).
	ResyncNode func(ctx context.Context, nodeGRPCAddr string) error
}

// CarrierPoolReplenisher runs off the hot path in the background:
// 1. Monitors idle Carrier Pod counts per node in Redis and tops up deficit Pods asynchronously;
// 2. Deletes Tainted (used) Carrier Pods asynchronously so zero K8s API / etcd calls ever block foreground requests;
// 3. Implements ColdSlotAllocator (direct NodeName-bound Pod creation bypassing kube-scheduler) on pool exhaustion.
type CarrierPoolReplenisher struct {
	kube      kubernetes.Interface
	catalog   store.RuntimeCatalog
	cfg       ReplenisherConfig
	taintedCh chan taintedPodRef
	// createSem bounds concurrent Pod creates so cold-burst storms do not stall CNI/PLEG.
	createSem chan struct{}
}

type taintedPodRef struct {
	uid       types.UID
	namespace string
	podName   string
}

func NewCarrierPoolReplenisher(kube kubernetes.Interface, catalog store.RuntimeCatalog, cfg ReplenisherConfig) *CarrierPoolReplenisher {
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.CarrierImage == "" {
		cfg.CarrierImage = "kontinue/carrier:dev"
	}
	if cfg.RuntimeClassName == "" {
		cfg.RuntimeClassName = protocol.RuntimeClassName
	}
	if cfg.MinIdlePerNode <= 0 {
		cfg.MinIdlePerNode = 4
	}
	if cfg.IdleBurstBuffer < 0 {
		cfg.IdleBurstBuffer = 0
	}
	// ColdBurstExtra defaults to 0: speculative extras help on large pools but
	// stampede Kind/CNI when every concurrent cold allocate also creates +N pods.
	if cfg.ColdBurstExtra < 0 {
		cfg.ColdBurstExtra = 0
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &CarrierPoolReplenisher{
		kube:      kube,
		catalog:   catalog,
		cfg:       cfg,
		taintedCh: make(chan taintedPodRef, 2048),
		createSem: make(chan struct{}, 8),
	}
}

func (r *CarrierPoolReplenisher) TargetIdlePerNode() int {
	return r.cfg.MinIdlePerNode + r.cfg.IdleBurstBuffer
}

func (r *CarrierPoolReplenisher) EnqueueTaintedPod(namespace, podName, podUID string) {
	if r == nil || r.kube == nil || podName == "" || podUID == "" {
		return
	}
	if namespace == "" {
		namespace = r.cfg.Namespace
	}
	select {
	case r.taintedCh <- taintedPodRef{namespace: namespace, podName: podName, uid: types.UID(podUID)}:
	default:
	}
}

func (r *CarrierPoolReplenisher) Start(ctx context.Context) {
	if r == nil || r.kube == nil {
		return
	}
	for w := 0; w < 8; w++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case item := <-r.taintedCh:
					grace := int64(0)
					_ = r.kube.CoreV1().Pods(item.namespace).Delete(ctx, item.podName, metav1.DeleteOptions{
						GracePeriodSeconds: &grace,
						Preconditions:      &metav1.Preconditions{UID: &item.uid},
					})
				}
			}
		}()
	}
	go func() {
		ticker := time.NewTicker(r.cfg.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.reconcilePoolWatermarks(ctx)
			}
		}
	}()
}

func (r *CarrierPoolReplenisher) reconcilePoolWatermarks(ctx context.Context) {
	if r.cfg.Leader != nil {
		release, ok, err := r.cfg.Leader.TryLeader(ctx, protocol.LeaderReplenisher)
		if err != nil || !ok {
			return
		}
		defer release()
	}
	nodes, err := r.catalog.ListHealthyNodes(ctx)
	if err != nil {
		return
	}
	target := r.TargetIdlePerNode()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, n := range nodes {
		idle, err := r.catalog.CountIdleSlots(ctx, n.NodeID, protocol.DefaultCarrierTier)
		if err != nil || idle >= target {
			continue
		}
		deficit := target - idle
		for i := 0; i < deficit; i++ {
			nodeID := n.NodeID
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				_, _ = r.createDirectBoundPod(ctx, nodeID, protocol.DefaultCarrierTier)
			}()
		}
	}
	wg.Wait()
}

func (r *CarrierPoolReplenisher) AllocateColdCarrierSlot(ctx context.Context, node protocol.NodeHeartbeat, tier, lifecycleID string) (*store.SlotReservation, error) {
	if r.kube == nil {
		return nil, store.ErrNoIdleCarrierSlot
	}
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "replenisher.allocate_cold_carrier",
		attribute.String(telemetry.AttrNodeID, node.NodeID),
		attribute.Int("carrier.cold_burst_extra", r.cfg.ColdBurstExtra),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	toCreate := 1 + r.cfg.ColdBurstExtra
	var createWG sync.WaitGroup
	errs := make([]error, toCreate)
	for i := 0; i < toCreate; i++ {
		createWG.Add(1)
		go func(idx int) {
			defer createWG.Done()
			_, err := r.createDirectBoundPod(ctx, node.NodeID, tier)
			errs[idx] = err
		}(i)
	}
	createWG.Wait()
	if errs[0] != nil {
		retErr = errs[0]
		return nil, retErr
	}

	if r.cfg.ResyncNode != nil && node.GRPCAddr != "" {
		_ = r.cfg.ResyncNode(ctx, node.GRPCAddr)
	}

	waitCtx, waitSpan := telemetry.StartSpan(ctx, "kontinue-apiserver", "replenisher.wait_slot_ready",
		attribute.String(telemetry.AttrNodeID, node.NodeID),
	)
	deadline := time.Now().Add(30 * time.Second)
	var slot *store.SlotReservation
	for time.Now().Before(deadline) {
		if waitCtx.Err() != nil {
			retErr = waitCtx.Err()
			telemetry.EndSpan(waitSpan, retErr)
			return nil, retErr
		}
		if claimed, cErr := r.catalog.ReserveIdleSlot(waitCtx, node.NodeID, tier, lifecycleID, true); cErr == nil && claimed != nil {
			slot = claimed
			break
		}
		if r.cfg.ResyncNode != nil && node.GRPCAddr != "" {
			_ = r.cfg.ResyncNode(waitCtx, node.GRPCAddr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if slot == nil {
		retErr = fmt.Errorf("cold carrier fallback on %s timed out waiting for watcher registration", node.NodeID)
		telemetry.EndSpan(waitSpan, retErr)
		return nil, retErr
	}
	telemetry.EndSpan(waitSpan, nil)
	span.SetAttributes(attribute.String(telemetry.AttrSlotID, slot.Slot.SlotID))
	return slot, nil
}

func (r *CarrierPoolReplenisher) createDirectBoundPod(ctx context.Context, nodeID, tier string) (*corev1.Pod, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "replenisher.create_carrier_pod",
		attribute.String(telemetry.AttrNodeID, nodeID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	if r.createSem != nil {
		select {
		case <-ctx.Done():
			retErr = ctx.Err()
			return nil, retErr
		case r.createSem <- struct{}{}:
			defer func() { <-r.createSem }()
		}
	}

	pod := newCarrierPod(r.cfg.Namespace, r.cfg.CarrierImage, nodeID, tier)
	created, err := r.kube.CoreV1().Pods(r.cfg.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		retErr = err
		return nil, err
	}
	span.SetAttributes(attribute.String("carrier.pod_name", created.Name))
	return created, nil
}

func newCarrierPod(namespace, image, nodeID, tier string) *corev1.Pod {
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	if image == "" {
		image = "kontinue/carrier:dev"
	}
	suffixBytes := make([]byte, 4)
	_, _ = rand.Read(suffixBytes)
	name := fmt.Sprintf("kt-carrier-%s", hex.EncodeToString(suffixBytes))
	nonRoot := int64(65535)

	// Carrier Pods run kontinue-carrier (pause) on default runc to hold Kubelet podManager
	// identity and CNI eth0 NetNS at ~100KB RSS. No shell / sleep loop — image ENTRYPOINT only.
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				protocol.LabelRole:        protocol.RoleCarrierPod,
				protocol.LabelCarrierTier: tier,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                     nodeID,
			RestartPolicy:                corev1.RestartPolicyAlways,
			AutomountServiceAccountToken: boolPtr(false),
			Tolerations: []corev1.Toleration{
				{
					Key:      "dedicated",
					Operator: corev1.TolerationOpEqual,
					Value:    "kontinue-sandbox",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			Containers: []corev1.Container{
				{
					Name:            protocol.ContainerNameSandbox,
					Image:           image,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1m"),
							corev1.ResourceMemory: resource.MustParse("8Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsNonRoot:             boolPtr(true),
						RunAsUser:                &nonRoot,
						RunAsGroup:               &nonRoot,
						AllowPrivilegeEscalation: boolPtr(false),
						ReadOnlyRootFilesystem:   boolPtr(true),
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					},
				},
			},
		},
	}
}

func boolPtr(v bool) *bool { return &v }
