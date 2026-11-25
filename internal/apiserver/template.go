package apiserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

func (s *Server) BuildGoldenTemplate(ctx context.Context, req CreateTemplateRequest) (_ *store.TemplateRecord, retErr error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.build_golden_template", attribute.String("template.name", req.Name))
	defer func() { telemetry.EndSpan(span, retErr) }()
	if req.TemplateID == "" {
		req.TemplateID = "tpl-" + randHex(6)
	}
	if req.Tier == "" {
		req.Tier = protocol.DefaultCarrierTier
	}
	unlock, err := s.lockTemplate(ctx, req.TemplateID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	now := time.Now().UTC()
	t := &store.TemplateRecord{TenantID: tenantID(ctx), TemplateID: req.TemplateID, Name: req.Name, BaseImage: req.BaseImage, Tier: req.Tier, Version: 1, WarmupScript: req.WarmupScript, PinInL1Tmpfs: req.PinInL1Tmpfs, TraceHotPages: req.TraceHotPages, Phase: "Allocating", BuildLogs: []string{"build requested"},
		Build: &store.TemplateBuild{SnapshotID: "snap-golden-" + randHex(12), LifecycleID: "life-" + randHex(12), AccessToken: "warmup-" + randHex(16), BootstrapToken: "boot-" + randHex(16), StartedAt: now}}
	if err := s.cfg.Meta.CreateTemplate(ctx, t); err != nil {
		return nil, err
	}
	return s.advanceTemplateBuild(ctx, t)
}

func (s *Server) RebuildGoldenTemplate(ctx context.Context, templateID string, req CreateTemplateRequest) (_ *store.TemplateRecord, retErr error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "apiserver.rebuild_golden_template", attribute.String("template.id", templateID))
	defer func() { telemetry.EndSpan(span, retErr) }()
	unlock, err := s.lockTemplate(ctx, templateID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	t, err := s.template(ctx, templateID)
	if err != nil {
		return nil, err
	}
	if t.Build != nil || t.Phase == "Allocating" || t.Phase == "Starting" || t.Phase == "WarmingUp" || t.Phase == store.TemplateCapturing || t.Phase == "Releasing" {
		return nil, store.ErrTransitionLocked
	}
	if req.Name != "" {
		t.Name = req.Name
	}
	if req.BaseImage != "" {
		t.BaseImage = req.BaseImage
	}
	if req.WarmupScript != "" {
		t.WarmupScript = req.WarmupScript
	}
	if req.Tier != "" {
		t.Tier = req.Tier
	}
	t.PinInL1Tmpfs = req.PinInL1Tmpfs
	t.TraceHotPages = req.TraceHotPages
	if t.Version == 0 {
		t.Version = 1
	}
	t.Version++
	t.Phase = "Allocating"
	t.StatusMessage = ""
	t.BuildLogs = []string{fmt.Sprintf("build %d requested", t.Version)}
	t.Build = &store.TemplateBuild{SnapshotID: "snap-golden-" + randHex(12), LifecycleID: "life-" + randHex(12), AccessToken: "warmup-" + randHex(16), BootstrapToken: "boot-" + randHex(16), StartedAt: time.Now().UTC()}
	if err := s.cfg.Meta.UpdateTemplate(ctx, t); err != nil {
		return nil, err
	}
	return s.advanceTemplateBuild(ctx, t)
}

func appendTemplateLog(t *store.TemplateRecord, message string) {
	if message == "" {
		return
	}
	t.BuildLogs = append(t.BuildLogs, message)
	if len(t.BuildLogs) > 100 {
		t.BuildLogs = t.BuildLogs[len(t.BuildLogs)-100:]
	}
}

func recordTemplateBuild(t *store.TemplateRecord, failed bool) {
	if t.Build == nil {
		return
	}
	completed := time.Now().UTC()
	phase := "Ready"
	if failed {
		phase = "Failed"
	}
	record := store.TemplateBuildRecord{Version: t.Version, SnapshotID: t.Build.SnapshotID, Phase: phase, StatusMessage: t.StatusMessage, Logs: append([]string(nil), t.BuildLogs...), CreatedAt: t.Build.StartedAt, CompletedAt: completed}
	t.History = append(t.History, record)
	if len(t.History) > 50 {
		t.History = t.History[len(t.History)-50:]
	}
}

func (s *Server) lockTemplate(ctx context.Context, id string) (func(), error) {
	unlock, ok, err := s.cfg.Meta.TryLeader(ctx, "template/"+id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, store.ErrTransitionLocked
	}
	return unlock, nil
}
func (s *Server) reconcileTemplateBuilds(ctx context.Context) {
	ids, err := s.cfg.Meta.ListPendingTemplateIDs(ctx)
	if err != nil {
		log.Printf("list pending template builds: %v", err)
		return
	}
	for _, id := range ids {
		unlock, err := s.lockTemplate(ctx, id)
		if err != nil {
			continue
		}
		t, err := s.cfg.Meta.GetTemplate(ctx, id)
		if err == nil && t.Build != nil {
			_, err = s.advanceTemplateBuild(ctx, t)
		}
		unlock()
		if err != nil {
			log.Printf("reconcile template %s: %v", id, err)
		}
	}
}

func (s *Server) advanceTemplateBuild(ctx context.Context, t *store.TemplateRecord) (*store.TemplateRecord, error) {
	b := t.Build
	for {
		switch t.Phase {
		case "Allocating":
			dec, err := s.cfg.Placement.SelectAndClaimSlot(ctx, PlacementRequest{Tier: t.Tier, LifecycleID: b.LifecycleID})
			if err != nil {
				return nil, err
			}
			b.Slot = dec.Slot
			b.Slot.NodeID, b.Slot.NodeIP, b.Slot.NodeGRPCAddr = dec.Node.NodeID, dec.Node.NodeIP, dec.Node.GRPCAddr
			b.PeerAddr = dec.Node.P2PAddr
			t.Phase = "Starting"
			appendTemplateLog(t, "carrier allocated")
		case "Starting":
			if err := s.cfg.Catalog.FinishReservation(ctx, b.LifecycleID, false); err != nil {
				return nil, err
			}
			nc, err := s.nodeClient(ctx, b.Slot.NodeGRPCAddr)
			if err != nil {
				return nil, err
			}
			res, err := nc.HydrateSlot(ctx, &protocol.HydrateSlotRequest{Slot: b.Slot, SandboxID: "builder-" + t.TemplateID, ExecutionID: 1, LifecycleID: b.LifecycleID, TemplateID: t.TemplateID, BaseImage: t.BaseImage, RestoreMode: protocol.RestoreModeCold, BootstrapToken: b.BootstrapToken, AccessToken: b.AccessToken, DeferRoutePublication: true})
			if err != nil {
				return nil, err
			}
			b.GuestEndpoint = res.PodIP
			appendTemplateLog(t, "carrier started")
			// A warmup is arbitrary user code. Record the attempt before invoking
			// it so the reconciler never repeats ambiguous external side effects.
			if t.WarmupScript != "" {
				t.Phase = "WarmingUp"
				appendTemplateLog(t, "warmup started")
				if err := s.cfg.Meta.SaveTemplateBuild(ctx, t); err != nil {
					return nil, err
				}
				out, err := s.ktdClient.Exec(ctx, b.GuestEndpoint, b.AccessToken, protocol.ExecRequest{Command: []string{"/bin/sh", "-c", t.WarmupScript}, TimeoutMs: 120000})
				if err != nil || out.ExitCode != 0 {
					b.Failed = true
					t.StatusMessage = "warmup command failed"
					if out != nil {
						t.StatusMessage = fmt.Sprintf("warmup command exited with status %d", out.ExitCode)
					}
					t.Phase = "Releasing"
					appendTemplateLog(t, "warmup failed")
					break
				}
				appendTemplateLog(t, "warmup completed")
			}
			t.Phase = store.TemplateCapturing
			appendTemplateLog(t, "capturing snapshot")
		case "WarmingUp":
			b.Failed = true
			t.StatusMessage = "warmup was interrupted before its outcome was recorded"
			t.Phase = "Releasing"
			appendTemplateLog(t, "warmup interrupted")
		case store.TemplateCapturing:
			if err := s.captureTemplate(ctx, t); err != nil {
				return nil, err
			}
			t.GoldenSnapshotID = b.SnapshotID
			t.Phase = "Releasing"
			appendTemplateLog(t, "snapshot captured")
		case "Releasing":
			nc, err := s.nodeClient(ctx, b.Slot.NodeGRPCAddr)
			if err != nil {
				return nil, err
			}
			receipt, err := nc.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: b.Slot.SlotID, SandboxID: "builder-" + t.TemplateID, ExecutionID: 1, LifecycleID: b.LifecycleID, ReuseSlot: s.cfg.ReuseSlots})
			if err != nil {
				return nil, err
			}
			if !receipt.Released {
				return nil, fmt.Errorf("template builder release is unconfirmed")
			}
			if !s.cfg.ReuseSlots && s.cfg.Replenisher != nil {
				if err := s.cfg.Replenisher.DeleteTaintedPod(ctx, b.Slot.PodNamespace, b.Slot.PodName, receipt.PodUID); err != nil {
					return nil, err
				}
			}
			appendTemplateLog(t, "carrier released")
			failed := b.Failed
			recordTemplateBuild(t, failed)
			t.Build = nil
			if failed || t.GoldenSnapshotID == "" {
				t.Phase = "Failed"
			} else {
				if t.PinInL1Tmpfs {
					s.broadcastL1Prewarm(ctx, t.GoldenSnapshotID)
				}
				t.Phase = "Ready"
				t.StatusMessage = "golden memory snapshot ready"
			}
		default:
			return nil, fmt.Errorf("unexpected template build phase %q", t.Phase)
		}
		if err := s.cfg.Meta.SaveTemplateBuild(ctx, t); err != nil {
			return nil, err
		}
		if t.Phase == "Ready" {
			return t, nil
		}
		if t.Phase == "Failed" {
			return nil, errors.New(t.StatusMessage)
		}
	}
}

func (s *Server) captureTemplate(ctx context.Context, t *store.TemplateRecord) error {
	b := t.Build
	if _, err := s.cfg.Meta.GetSnapshot(ctx, b.SnapshotID); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	prep := &protocol.PrepareCheckpointRequest{SandboxID: "builder-" + t.TemplateID, ExecutionID: 1, LifecycleID: b.LifecycleID, SlotID: b.Slot.SlotID, PodNamespace: b.Slot.PodNamespace, PodName: b.Slot.PodName, PodIP: b.Slot.PodIP,
		TargetSnapshotID: b.SnapshotID, Kind: protocol.SnapshotKindGoldenTemplate, TemplateID: t.TemplateID, BaseImage: t.BaseImage, BootstrapToken: b.BootstrapToken, LeaveRunning: true, TraceHotPages: t.TraceHotPages}
	_, err := s.publishCheckpoint(ctx, prep, b.Slot.NodeID, b.Slot.NodeIP, b.Slot.NodeGRPCAddr, b.PeerAddr, checkpointCaptureIfMissing)
	return err
}
