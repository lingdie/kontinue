package apiserver

import (
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func TestNewCarrierPodHasNoSleepCommand(t *testing.T) {
	pod := newCarrierPod("kontinue-sandboxes", "kontinue/carrier:dev", "worker-1", protocol.DefaultCarrierTier)
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if len(c.Command) != 0 || len(c.Args) != 0 {
		t.Fatalf("carrier must use image ENTRYPOINT only; got command=%v args=%v", c.Command, c.Args)
	}
	if c.Image != "kontinue/carrier:dev" {
		t.Fatalf("unexpected image %q", c.Image)
	}
	if c.Resources.Requests.Memory().IsZero() {
		t.Fatal("expected memory request")
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil || *c.SecurityContext.RunAsUser != 65535 {
		t.Fatal("expected non-root uid 65535")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("carrier must not mount a service account token")
	}
}

func TestReplenisherTargetIdleIncludesBurstBuffer(t *testing.T) {
	r := NewCarrierPoolReplenisher(nil, nil, ReplenisherConfig{
		MinIdlePerNode:  6,
		IdleBurstBuffer: 2,
		ColdBurstExtra:  0,
	})
	if got := r.TargetIdlePerNode(); got != 8 {
		t.Fatalf("TargetIdlePerNode=%d want 8", got)
	}
	if r.cfg.ColdBurstExtra != 0 {
		t.Fatalf("ColdBurstExtra=%d want 0", r.cfg.ColdBurstExtra)
	}
}
