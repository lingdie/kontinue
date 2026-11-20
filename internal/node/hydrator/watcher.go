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

	mu   sync.Mutex
	seen map[string]protocol.CarrierSlot
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
	}
}

func (w *CarrierPodWatcher) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(w.cfg.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.syncOnce(ctx)
			}
		}
	}()
}

// ForceSync runs an immediate Carrier discovery pass (used after cold Pod creates).
func (w *CarrierPodWatcher) ForceSync(ctx context.Context) {
	if w == nil {
		return
	}
	w.syncOnce(ctx)
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
	for _, sb := range resp.Items {
		activeIDs[sb.Id] = true

		w.mu.Lock()
		_, alreadySeen := w.seen[sb.Id]
		w.mu.Unlock()
		if alreadySeen || (w.cfg.IsClaimed != nil && w.cfg.IsClaimed("slot-"+sb.Metadata.Name)) {
			continue
		}

		stResp, err := w.rt.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{
			PodSandboxId: sb.Id,
			Verbose:      true,
		})
		if err != nil || stResp.Status == nil || stResp.Status.Network == nil || stResp.Status.Network.Ip == "" {
			continue
		}
		podIP := stResp.Status.Network.Ip
		netnsPath := extractNetNSPath(stResp.Info)
		if netnsPath == "" {
			continue
		}
		// Never register a Carrier Pod into the idle pool if a live ktd sandbox is already listening on :49983!
		if conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", podIP, protocol.DefaultKtdPort), 40*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			w.mu.Lock()
			w.seen[sb.Id] = protocol.CarrierSlot{SlotID: "slot-" + sb.Metadata.Name}
			w.mu.Unlock()
			continue
		}

		ctrsResp, err := w.rt.ListContainers(ctx, &runtimeapi.ListContainersRequest{
			Filter: &runtimeapi.ContainerFilter{
				PodSandboxId: sb.Id,
				State:        &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING},
			},
		})
		if err != nil || len(ctrsResp.Containers) == 0 {
			continue
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

		if err := w.catalog.RegisterIdleSlot(ctx, slot); err == nil {
			w.mu.Lock()
			w.seen[sb.Id] = slot
			w.mu.Unlock()
			log.Printf("kontinued registered idle Carrier Slot %s (pod=%s/%s ip=%s netns=%s)",
				slot.SlotID, slot.PodNamespace, slot.PodName, slot.PodIP, slot.NetNSPath)
		}
	}

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
