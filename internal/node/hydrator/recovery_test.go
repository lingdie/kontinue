package hydrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type inspectingDriver struct {
	recordingDriver
	inspectErr error
}

func (d *inspectingDriver) inspectInstance(context.Context, ownership) error { return d.inspectErr }

func TestRecoverChecksRuntimeAndGuestBeforeCheckpointAndRelease(t *testing.T) {
	for _, tc := range []struct {
		name            string
		runtimeErr      error
		guestGeneration int64
		adopt           bool
	}{
		{"matching", nil, 3, true}, {"obsolete guest", nil, 4, false}, {"runtime gone", errInstanceLost, 3, false}, {"reassigned runtime", errWrongOwner, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(protocol.HealthResponse{Healthy: true, Initialized: true, SandboxID: "box", ExecutionID: tc.guestGeneration})
			}))
			defer guest.Close()
			ctx := context.Background()
			d := &inspectingDriver{inspectErr: tc.runtimeErr}
			s := NewService(ServiceConfig{ScratchDir: t.TempDir(), Driver: d, NodeID: "worker"})
			o := ownership{Version: 1, Phase: activeExecution, Slot: protocol.CarrierSlot{NodeID: "worker", SlotID: "slot"}, SandboxID: "box", ExecutionID: 3, LifecycleID: "life", ContainerID: "kt-test", KtdEndpoint: strings.TrimPrefix(guest.URL, "http://")}
			if err := s.journal.write(o, true); err != nil {
				t.Fatal(err)
			}
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			_, err := s.PrepareCheckpoint(ctx, &protocol.PrepareCheckpointRequest{SlotID: "slot", SandboxID: "box", ExecutionID: 3, LifecycleID: "life"})
			if (err == nil) != tc.adopt {
				t.Fatalf("checkpoint admission %v", err)
			}
			claims, err := s.journal.load("worker")
			if err != nil || len(claims) != 1 {
				t.Fatalf("claims=%v err=%v", claims, err)
			}
			if tc.adopt {
				if claims[0].Phase != activeExecution {
					t.Fatal(claims)
				}
				if _, err := s.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: "slot", SandboxID: "box", ExecutionID: 3}); err != nil || len(d.stopped) != 1 {
					t.Fatalf("release %v stops %v", err, d.stopped)
				}
			} else if claims[0].Phase != lost || claims[0].FailureReason == "" || len(d.stopped) > 0 {
				t.Fatal(claims)
			}
			if errors.Is(tc.runtimeErr, errWrongOwner) {
				if _, err := s.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: "slot", SandboxID: "box", ExecutionID: 3}); !errors.Is(err, errWrongOwner) || len(d.stopped) > 0 {
					t.Fatalf("deleted conflicting runtime: %v", err)
				}
			}
		})
	}
}
