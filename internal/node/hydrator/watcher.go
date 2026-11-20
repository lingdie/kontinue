package hydrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type WatcherConfig struct {
	IsClaimed    func(string) bool
	NodeID       string
	NodeIP       string
	NodeGRPCAddr string
	PollInterval time.Duration
}

// CarrierPodWatcher discovers ready Carrier Pods on the local worker node via containerd CRI,
// extracts their CNI PodIP, network namespace path, and placeholder container ID, and registers
// them into Redis's idle Carrier Slot pool automatically.
type CarrierPodWatcher struct {
	rt      runtimeapi.RuntimeServiceClient
	catalog store.RuntimeCatalog
	cfg     WatcherConfig

	mu    sync.Mutex
	seen  map[string]protocol.CarrierSlot
	wake  chan struct{}
	syncs singleflight.Group
}

func NewCarrierPodWatcher(rt runtimeapi.RuntimeServiceClient, catalog store.RuntimeCatalog, cfg WatcherConfig) *CarrierPodWatcher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	return &CarrierPodWatcher{
		rt:      rt,
		catalog: catalog,
		cfg:     cfg,
		seen:    make(map[string]protocol.CarrierSlot),
		wake:    make(chan struct{}, 1),
	}
}

func (w *CarrierPodWatcher) Start(ctx context.Context) {
	go w.watchCRIEvents(ctx)
	go func() {
		ticker := time.NewTicker(w.cfg.PollInterval)
		defer ticker.Stop()
		w.ForceSync(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.ForceSync(ctx)
			case <-w.wake:
				w.ForceSync(ctx)
			}
		}
	}()
}

func (w *CarrierPodWatcher) watchCRIEvents(ctx context.Context) {
	for ctx.Err() == nil {
		stream, err := w.rt.GetContainerEvents(ctx, &runtimeapi.GetEventsRequest{})
		for err == nil {
			event, recvErr := stream.Recv()
			err = recvErr
			if err != nil {
				break
			}
			if event.GetContainerEventType() == runtimeapi.ContainerEventType_CONTAINER_STARTED_EVENT &&
				event.GetPodSandboxStatus().GetLabels()[protocol.LabelRole] == protocol.RoleCarrierPod {
				select {
				case w.wake <- struct{}{}:
				default:
				}
			}
		}
		// Older or temporarily unavailable runtimes still have periodic scans.
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

// ForceSync coalesces concurrent requests from older API server replicas.
func (w *CarrierPodWatcher) ForceSync(ctx context.Context) {
	if w == nil {
		return
	}
	w.syncs.Do("carrier discovery", func() (any, error) {
		w.syncOnce(ctx)
		return nil, nil
	})
}

func (w *CarrierPodWatcher) syncOnce(ctx context.Context) {
	resp, err := w.rt.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{
			State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_READY},
			LabelSelector: map[string]string{
				protocol.LabelRole: protocol.RoleCarrierPod,
			},
		},
	})
	if err != nil {
		return
	}

	activeIDs := make(map[string]bool, len(resp.Items))
	var checks errgroup.Group
	checks.SetLimit(4)
	for _, sb := range resp.Items {
		activeIDs[sb.Id] = true

		w.mu.Lock()
		_, alreadySeen := w.seen[sb.Id]
		w.mu.Unlock()
		if alreadySeen || (w.cfg.IsClaimed != nil && w.cfg.IsClaimed("slot-"+sb.Metadata.Name)) {
			continue
		}

		checks.Go(func() error {
			w.registerCarrier(ctx, sb)
			return nil
		})
	}

	_ = checks.Wait()

	var stale []protocol.CarrierSlot
	w.mu.Lock()
	for id, slot := range w.seen {
		if !activeIDs[id] {
			stale = append(stale, slot)
			delete(w.seen, id)
		}
	}
	w.mu.Unlock()
	for _, s := range stale {
		if s.SlotID != "" {
			_ = w.catalog.RemoveIdleSlot(ctx, w.cfg.NodeID, s.Tier, s.SlotID)
		}
	}
}

func (w *CarrierPodWatcher) registerCarrier(ctx context.Context, sb *runtimeapi.PodSandbox) {
	stResp, err := w.rt.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{
		PodSandboxId: sb.Id,
		Verbose:      true,
	})
	if err != nil || stResp.Status == nil || stResp.Status.Network == nil || stResp.Status.Network.Ip == "" {
		return
	}
	podIP := stResp.Status.Network.Ip
	netnsPath := extractNetNSPath(stResp.Info)
	if netnsPath == "" {
		return
	}
	// Never register a Carrier Pod into the idle pool if a live ktd sandbox is already listening on :49983!
	if conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", podIP, protocol.DefaultKtdPort), 40*time.Millisecond); dialErr == nil {
		_ = conn.Close()
		w.mu.Lock()
		w.seen[sb.Id] = protocol.CarrierSlot{SlotID: "slot-" + sb.Metadata.Name}
		w.mu.Unlock()
		return
	}

	ctrsResp, err := w.rt.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			PodSandboxId: sb.Id,
			State:        &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING},
		},
	})
	if err != nil || len(ctrsResp.Containers) == 0 {
		return
	}
	var placeholder *runtimeapi.Container
	for _, c := range ctrsResp.Containers {
		if c.Metadata != nil && c.Metadata.Name == protocol.ContainerNameSandbox {
			placeholder = c
			break
		}
	}
	if placeholder == nil {
		placeholder = ctrsResp.Containers[0]
	}

	var logPath string
	if cSt, cErr := w.rt.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: placeholder.Id}); cErr == nil && cSt.Status != nil {
		logPath = cSt.Status.LogPath
	}

	tier := sb.Labels[protocol.LabelCarrierTier]
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	slot := protocol.CarrierSlot{
		SlotID:                "slot-" + sb.Metadata.Name,
		NodeID:                w.cfg.NodeID,
		NodeIP:                w.cfg.NodeIP,
		NodeGRPCAddr:          w.cfg.NodeGRPCAddr,
		Tier:                  tier,
		PodNamespace:          sb.Metadata.Namespace,
		PodName:               sb.Metadata.Name,
		PodUID:                sb.Metadata.Uid,
		PodIP:                 podIP,
		PodSandboxID:          sb.Id,
		NetNSPath:             netnsPath,
		PlaceholderID:         placeholder.Id,
		ExpectedContainerHash: placeholder.Labels[protocol.K8sLabelContainerHash],
		LogPath:               logPath,
	}

	if err := w.catalog.RegisterColdCarrier(ctx, slot, sb.Labels[protocol.LabelColdLifecycle]); err == nil {
		w.mu.Lock()
		w.seen[sb.Id] = slot
		w.mu.Unlock()
		log.Printf("kontinued registered idle Carrier Slot %s (pod=%s/%s ip=%s netns=%s)",
			slot.SlotID, slot.PodNamespace, slot.PodName, slot.PodIP, slot.NetNSPath)
	}
}

func extractNetNSPath(info map[string]string) string {
	raw, ok := info["info"]
	if !ok || raw == "" {
		return ""
	}
	var parsed struct {
		PID         int `json:"pid"`
		RuntimeSpec struct {
			Linux struct {
				Namespaces []struct {
					Type string `json:"type"`
					Path string `json:"path"`
				} `json:"namespaces"`
			} `json:"linux"`
		} `json:"runtimeSpec"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return ""
	}
	for _, ns := range parsed.RuntimeSpec.Linux.Namespaces {
		if ns.Type == "network" && ns.Path != "" {
			return ns.Path
		}
	}
	if parsed.PID > 0 {
		return fmt.Sprintf("/proc/%d/ns/net", parsed.PID)
	}
	return ""
}
