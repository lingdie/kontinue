package hydrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type activationDriver struct {
	inspectingDriver
	endpoint string
	starts   atomic.Int32
}

type activationCarrier struct {
	runtimeapi.RuntimeServiceClient
	slot protocol.CarrierSlot
}

func (c activationCarrier) PodSandboxStatus(_ context.Context, req *runtimeapi.PodSandboxStatusRequest, _ ...grpc.CallOption) (*runtimeapi.PodSandboxStatusResponse, error) {
	if req.PodSandboxId != c.slot.PodSandboxID || !req.Verbose {
		return nil, status.Error(codes.NotFound, "unexpected Carrier query")
	}
	return &runtimeapi.PodSandboxStatusResponse{
		Status: &runtimeapi.PodSandboxStatus{
			Id: c.slot.PodSandboxID, State: runtimeapi.PodSandboxState_SANDBOX_READY,
			Metadata: &runtimeapi.PodSandboxMetadata{Uid: c.slot.PodUID, Name: c.slot.PodName, Namespace: c.slot.PodNamespace},
			Network:  &runtimeapi.PodSandboxNetworkStatus{Ip: c.slot.PodIP},
		},
		Info: map[string]string{"info": `{"pid":123}`},
	}, nil
}

func (d *activationDriver) SwapAndStartSandboxContainer(_ context.Context, o SwapContainerOptions) (string, string, error) {
	d.starts.Add(1)
	return o.ContainerID, d.endpoint, nil
}

func TestReplayActivationAfterLostReplyAndDaemonReplacement(t *testing.T) {
	ctx := context.Background()
	var inits atomic.Int32
	scratch := t.TempDir()
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == protocol.PathInternalInit {
			_, _ = io.Copy(io.Discard, r.Body)
			inits.Add(1)
			claims, err := ownershipJournal(filepath.Join(scratch, "ownership")).load("node")
			if err != nil || len(claims) != 1 || claims[0].Phase != preparing || claims[0].Activation == nil {
				t.Error("activation reply fields were not staged before HeldInit", err)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, `{"healthy":true,"initialized":true,"sandboxId":"box","executionId":2}`)
	}))
	defer guest.Close()
	catalog := store.NewMemoryRuntimeCatalog()
	d := &activationDriver{endpoint: strings.TrimPrefix(guest.URL, "http://")}
	cfg := ServiceConfig{NodeID: "node", ScratchDir: scratch, Driver: d, Routes: catalog}
	s := NewService(cfg)
	req := &protocol.HydrateSlotRequest{Slot: protocol.CarrierSlot{SlotID: "slot", NodeID: "node"}, SandboxID: "box", ExecutionID: 2, LifecycleID: "life", AccessToken: "secret", BaseImage: "image", DeferRoutePublication: true}
	first, err := s.HydrateSlot(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GetRoute(ctx, "box"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("uncommitted route: %v", err)
	}
	s = NewService(cfg)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := s.HydrateSlot(ctx, req)
	if err != nil || !reflect.DeepEqual(first, second) || inits.Load() != 1 || d.starts.Load() != 1 {
		t.Fatalf("replay %v, %v; starts=%d inits=%d", second, err, d.starts.Load(), inits.Load())
	}
	conflict := *req
	conflict.BaseImage = "other"
	if _, err := s.HydrateSlot(ctx, &conflict); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflicting arguments: %v", err)
	}
	if _, err := s.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: "slot", SandboxID: "box", ExecutionID: 2, LifecycleID: "life"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HydrateSlot(ctx, req); status.Code(err) != codes.FailedPrecondition || d.starts.Load() != 1 {
		t.Fatalf("delayed hydrate after release: %v", err)
	}
}

func TestRecoverInterruptedActivation(t *testing.T) {
	for _, tc := range []struct {
		name, lifecycle, carrierUID string
		initialized, expired        bool
		wantReady                   bool
	}{
		{name: "HeldInit completed", lifecycle: "life", carrierUID: "uid", initialized: true, wantReady: true},
		{name: "same execution number but wrong lifecycle", lifecycle: "other", carrierUID: "uid", initialized: true},
		{name: "legacy guest without lifecycle proof", carrierUID: "uid", initialized: true},
		{name: "Carrier replaced", lifecycle: "life", carrierUID: "replaced", initialized: true},
		{name: "guest not initialized", lifecycle: "life", carrierUID: "uid"},
		{name: "guest unreachable after recovery deadline", lifecycle: "life", carrierUID: "uid", expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var healthCalls, otherCalls atomic.Int32
			guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != protocol.PathInternalHealth {
					otherCalls.Add(1)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				healthCalls.Add(1)
				if tc.expired {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_ = json.NewEncoder(w).Encode(protocol.HealthResponse{
					Healthy: true, Initialized: tc.initialized, SandboxID: "box", ExecutionID: 2, LifecycleID: tc.lifecycle,
				})
			}))
			defer guest.Close()
			endpoint := strings.TrimPrefix(guest.URL, "http://")
			ip, _, err := net.SplitHostPort(endpoint)
			if err != nil {
				t.Fatal(err)
			}
			slot := protocol.CarrierSlot{SlotID: "slot", NodeID: "node", PodNamespace: "ns", PodName: "carrier", PodSandboxID: "cri-id", PodUID: "uid", PodIP: ip, NetNSPath: "/proc/123/ns/net", NetworkReadyOnly: true}
			req := &protocol.HydrateSlotRequest{Slot: slot, SandboxID: "box", ExecutionID: 2, LifecycleID: "life", DeferRoutePublication: true}
			actualCarrier := slot
			actualCarrier.PodUID = tc.carrierUID
			driver := &activationDriver{endpoint: endpoint}
			catalog := store.NewMemoryRuntimeCatalog()
			s := NewService(ServiceConfig{NodeID: "node", ScratchDir: t.TempDir(), Driver: driver, Carriers: activationCarrier{slot: actualCarrier}, Routes: catalog})
			copiedBytes := int64(1024)
			staged := &protocol.HydrateSlotResponse{SandboxID: req.SandboxID, ExecutionID: req.ExecutionID, ContainerID: "kt-original", PodIP: endpoint, ArenaTier: "L2", RestoreMode: protocol.RestoreModeMemory, RestoreViewCopiedBytes: &copiedBytes}
			raw, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			claim := ownership{Version: 1, Phase: preparing, Slot: slot, SandboxID: req.SandboxID, ExecutionID: req.ExecutionID, LifecycleID: req.LifecycleID,
				ContainerID: staged.ContainerID, KtdEndpoint: endpoint, RequestHash: fmt.Sprintf("%x", sha256.Sum256(raw)), Activation: staged}
			if err := s.journal.write(claim, true); err != nil {
				t.Fatal(err)
			}
			if tc.expired {
				st := &activeSlotState{restored: true, ownership: claim, recoveryDeadline: time.Now()}
				s.activeSlots[slot.SlotID] = st
				if err := s.reconcile(ctx, st); err != nil {
					t.Fatalf("failed to record expired activation: %v", err)
				}
			} else if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			result, err := s.HydrateSlot(ctx, req)
			claims, loadErr := s.journal.load("node")
			if loadErr != nil || len(claims) != 1 {
				t.Fatalf("read recovered claim: %v %v", claims, loadErr)
			}
			report, reportErr := catalog.ExecutionReport(ctx, claim.route(s.cfg))
			if tc.wantReady {
				if err != nil || !reflect.DeepEqual(result, staged) || claims[0].Phase != activeExecution || !reflect.DeepEqual(claims[0].Activation, staged) || reportErr != nil || report.State != store.OwnerReady {
					t.Fatalf("activation was not recovered: result=%+v err=%v phase=%s report=%+v %v", result, err, claims[0].Phase, report, reportErr)
				}
			} else {
				if status.Code(err) != codes.FailedPrecondition || claims[0].Phase != lost || claims[0].FailureReason == "" || reportErr != nil || report.State != store.OwnerFailed {
					t.Fatalf("activation remained pending: result=%+v err=%v phase=%s report=%+v %v", result, err, claims[0].Phase, report, reportErr)
				}
			}
			if tc.carrierUID != slot.PodUID && healthCalls.Load() != 0 {
				t.Fatal("probed the guest on a replacement Carrier")
			}
			if driver.starts.Load() != 0 || otherCalls.Load() != 0 {
				t.Fatalf("recovery repeated activation: starts=%d guest mutations=%d", driver.starts.Load(), otherCalls.Load())
			}
			if _, err := catalog.GetRoute(ctx, claim.SandboxID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("published a route before the API committed activation: %v", err)
			}
			if !tc.wantReady {
				response, err := s.ReleaseSlot(ctx, &protocol.ReleaseSlotRequest{SlotID: slot.SlotID, SandboxID: claim.SandboxID, ExecutionID: claim.ExecutionID, LifecycleID: claim.LifecycleID})
				if tc.carrierUID != slot.PodUID {
					if !errors.Is(err, errWrongOwner) || len(driver.stopped) != 0 {
						t.Fatalf("released a conflicting Carrier: response=%+v err=%v stops=%v", response, err, driver.stopped)
					}
				} else if err != nil || response == nil || !response.Released || !reflect.DeepEqual(driver.stopped, []string{claim.ContainerID}) {
					t.Fatalf("could not release the verified runtime: response=%+v err=%v stops=%v", response, err, driver.stopped)
				}
			}
		})
	}
}
