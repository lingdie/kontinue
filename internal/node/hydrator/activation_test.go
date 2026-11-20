package hydrator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type activationDriver struct {
	inspectingDriver
	endpoint string
	starts   atomic.Int32
}

func (d *activationDriver) SwapAndStartSandboxContainer(_ context.Context, o SwapContainerOptions) (string, string, error) {
	d.starts.Add(1)
	return o.ContainerID, d.endpoint, nil
}

func TestReplayActivationAfterLostReplyAndDaemonReplacement(t *testing.T) {
	ctx := context.Background()
	var inits atomic.Int32
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == protocol.PathInternalInit {
			_, _ = io.Copy(io.Discard, r.Body)
			inits.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, `{"healthy":true,"initialized":true,"sandboxId":"box","executionId":2}`)
	}))
	defer guest.Close()
	catalog := store.NewMemoryRuntimeCatalog()
	d := &activationDriver{endpoint: strings.TrimPrefix(guest.URL, "http://")}
	cfg := ServiceConfig{NodeID: "node", ScratchDir: t.TempDir(), Driver: d, Routes: catalog}
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
