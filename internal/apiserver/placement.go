package apiserver

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

type PlacementRequest struct {
	LifecycleID                 string
	Tier                        string
	SnapshotID                  string
	OriginNodeID                string
	RequiredFingerprint         *protocol.CompatibilityFingerprint
	AllowFilesystemOnlyFallback bool
}

type PlacementDecision struct {
	Node         protocol.NodeHeartbeat
	Slot         protocol.CarrierSlot
	RestoreMode  string
	ColdFallback bool
}

// ColdSlotAllocator is invoked only when burst traffic exhausts all pre-warmed Carrier Pod slots in Redis,
// creating a direct-bound Carrier Pod (pod.Spec.NodeName = nodeID, bypassing kube-scheduler).
type ColdSlotAllocator interface {
	AllocateColdCarrierSlot(ctx context.Context, node protocol.NodeHeartbeat, tier, lifecycleID string) (*store.SlotReservation, error)
}

type PlacementEngine struct {
	catalog       store.RuntimeCatalog
	coldAllocator ColdSlotAllocator
}

func NewPlacementEngine(catalog store.RuntimeCatalog, coldAllocator ColdSlotAllocator) *PlacementEngine {
	return &PlacementEngine{
		catalog:       catalog,
		coldAllocator: coldAllocator,
	}
}

// SelectAndClaimSlot implements Best-of-K memory-aware placement:
//  1. Filters nodes by CPU/runsc CompatibilityFingerprint;
//  2. Ranks OriginNode (L2 NVMe hit) > L1 tmpfs pinned nodes > Free memory headroom;
//  3. Atomically claims an idle Carrier Slot via Redis Lua SPOP;
//  4. Falls back to cold Carrier allocation when the warm pool is empty.
//     Incompatible hardware is rejected; there is no filesystem-only restore.
func (p *PlacementEngine) SelectAndClaimSlot(ctx context.Context, req PlacementRequest) (*PlacementDecision, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "placement.select_and_claim_slot",
		attribute.String(telemetry.AttrSnapshotID, req.SnapshotID),
		attribute.String("origin.node_id", req.OriginNodeID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	if req.Tier == "" {
		req.Tier = protocol.DefaultCarrierTier
	}
	nodes, err := p.catalog.ListHealthyNodes(ctx)
	if err != nil {
		retErr = err
		return nil, err
	}
	if len(nodes) == 0 {
		retErr = fmt.Errorf("no healthy kontinued worker nodes registered")
		return nil, retErr
	}

	type scoredNode struct {
		hb    protocol.NodeHeartbeat
		score float64
		mode  string
	}

	var candidates []scoredNode
	for _, n := range nodes {
		if req.RequiredFingerprint != nil && req.SnapshotID != "" {
			if !req.RequiredFingerprint.CompatibleWith(n.Fingerprint) {
				continue
			}
		}
		// Add small jitter (0..1.0) so equal-score nodes balance burst traffic evenly instead ofherd-piling node 0!
		score := memoryHeadroomScore(n) + rand.Float64()
		if req.OriginNodeID != "" && n.NodeID == req.OriginNodeID {
			score += 1000.0
			if req.SnapshotID != "" && slices.Contains(n.L2Snapshots, req.SnapshotID) {
				score += 300.0
			}
		}
		if req.SnapshotID != "" && slices.Contains(n.L1Snapshots, req.SnapshotID) {
			score += 500.0
		}
		mode := protocol.RestoreModeMemory
		if req.SnapshotID == "" {
			mode = protocol.RestoreModeCold
		}
		candidates = append(candidates, scoredNode{hb: n, score: score, mode: mode})
	}

	if len(candidates) == 0 {
		retErr = fmt.Errorf("no hardware-compatible worker node matches snapshot fingerprint")
		return nil, retErr
	}

	slices.SortFunc(candidates, func(a, b scoredNode) int {
		if a.score > b.score {
			return -1
		}
		if a.score < b.score {
			return 1
		}
		return 0
	})

	decision := func(reservation *store.SlotReservation) (*PlacementDecision, error) {
		for _, cand := range candidates {
			if cand.hb.NodeID == reservation.Slot.NodeID {
				span.SetAttributes(attribute.String(telemetry.AttrNodeID, cand.hb.NodeID), attribute.String(telemetry.AttrSlotID, reservation.Slot.SlotID), attribute.String(telemetry.AttrRestoreMode, cand.mode), attribute.Bool("carrier.cold_fallback", reservation.Cold))
				return &PlacementDecision{Node: cand.hb, Slot: reservation.Slot, RestoreMode: cand.mode, ColdFallback: reservation.Cold}, nil
			}
		}
		return nil, fmt.Errorf("reserved slot %s is on an unavailable or incompatible worker", reservation.Slot.SlotID)
	}
	for _, cand := range candidates {
		r, err := p.catalog.ReserveIdleSlot(ctx, cand.hb.NodeID, req.Tier, req.LifecycleID, false)
		if err == nil {
			return decision(r)
		}
		if !errors.Is(err, store.ErrNoIdleCarrierSlot) {
			return nil, err
		}
	}
	if p.coldAllocator != nil {
		pick := candidates[rand.IntN(len(candidates))]
		r, err := p.coldAllocator.AllocateColdCarrierSlot(ctx, pick.hb, req.Tier, req.LifecycleID)
		if err != nil {
			return nil, err
		}
		return decision(r)
	}

	retErr = store.ErrNoIdleCarrierSlot
	return nil, retErr
}

func memoryHeadroomScore(n protocol.NodeHeartbeat) float64 {
	if n.TotalMemBytes <= 0 {
		return 50.0
	}
	return (float64(n.FreeMemBytes) / float64(n.TotalMemBytes)) * 100.0
}
