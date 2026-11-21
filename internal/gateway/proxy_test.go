package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type failedAdmissionCatalog struct {
	store.RuntimeCatalog
	err error
}

func (c failedAdmissionCatalog) AdmitRoute(context.Context, string, string) (*protocol.RouteRecord, error) {
	return nil, c.err
}

type failedProxyTransport struct{ err error }

func (t failedProxyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, t.err
}

func TestGatewayDependencyFailuresHideInternalDiagnostics(t *testing.T) {
	const token = "sandbox-token"
	const diagnostic = `request to "http://private-worker:49983" failed`
	internalErr := errors.New(diagnostic)
	for _, tc := range []struct {
		name, code, retryAfter string
		wantStatus             int
	}{
		{"admission unavailable", "sandbox_unavailable", "2", http.StatusServiceUnavailable},
		{"proxy transport failure", "sandbox_unreachable", "", http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := store.NewMemoryRuntimeCatalog()
			var routes store.RuntimeCatalog = catalog
			if tc.wantStatus == http.StatusServiceUnavailable {
				routes = failedAdmissionCatalog{RuntimeCatalog: catalog, err: internalErr}
			} else if err := catalog.PublishRoute(context.Background(), protocol.RouteRecord{
				SandboxID: "box", ExecutionID: 1, PodIP: "127.0.0.1", AccessDigest: protocol.AccessDigest(token),
			}); err != nil {
				t.Fatal(err)
			}
			gateway := NewGateway(Config{Catalog: routes})
			gateway.transport = failedProxyTransport{err: internalErr}
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set(protocol.HeaderKontinueSandboxID, "box")
			req.Header.Set(protocol.HeaderKontinueToken, token)
			w := httptest.NewRecorder()
			gateway.Handler().ServeHTTP(w, req)
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != tc.wantStatus || body["code"] != tc.code || body["error"] == "" {
				t.Fatalf("unexpected error response: HTTP %d %s (decode error: %v)", w.Code, w.Body.String(), err)
			}
			if strings.Contains(w.Body.String(), "private-worker") || w.Header().Get("Retry-After") != tc.retryAfter {
				t.Fatalf("unexpected public error: %s headers=%v", w.Body.String(), w.Header())
			}
		})
	}
}

func TestGatewayDropsIdleConnectionCounters(t *testing.T) {
	gateway := NewGateway(Config{})
	counter := gateway.counterFor("box")
	counter.Add(1)

	gateway.releaseCounter("box", counter)

	if got := gateway.ActiveConnections("box"); got != 0 {
		t.Fatalf("idle connection counter remained active: %d", got)
	}
	gateway.inFlightMu.Lock()
	defer gateway.inFlightMu.Unlock()
	if _, ok := gateway.inFlight["box"]; ok {
		t.Fatal("idle connection counter was retained")
	}
}
