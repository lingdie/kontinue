package apiserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lifecycleErrorStore struct {
	store.MetadataStore
	err error
}

func TestLifecycleRoutingErrorsKeepHTTPStatusAndChallenge(t *testing.T) {
	const credential = "tenant-test-token"
	s := NewServer(ServerConfig{Meta: store.NewMemoryMetadataStore(), Catalog: store.NewMemoryRuntimeCatalog(),
		TenantKeys: TenantKeys{sha256.Sum256([]byte(credential)): localTenant}})
	defer s.Close()
	for _, tc := range []struct {
		method, path  string
		authenticated bool
		status        int
		code          string
		challenge     string
	}{
		{"POST", "/v1/sandboxes", false, 401, "unauthorized", "Bearer"},
		{"GET", "/v1/sandboxes", true, 405, "method_not_allowed", ""},
		{"POST", "/v1/sandboxes/box/unknown", true, 404, "not_found", ""},
		{"POST", "/internal/gateway/sandboxes/box/resume", false, 401, "unauthorized", ""},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if tc.authenticated {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		var response struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != tc.status || response.Code != tc.code || w.Header().Get("WWW-Authenticate") != tc.challenge {
			t.Fatalf("%s %s: HTTP %d %s headers=%v (decode error: %v)", tc.method, tc.path, w.Code, w.Body.String(), w.Header(), err)
		}
	}
}

func (m lifecycleErrorStore) GetSandbox(context.Context, string) (*store.SandboxRecord, error) {
	return nil, m.err
}

func (m lifecycleErrorStore) ListTemplates(context.Context) ([]store.TemplateRecord, error) {
	return nil, m.err
}

func TestLifecycleErrorsKeepDetailsInServerLogs(t *testing.T) {
	const detail = "private-diagnostic-token and /internal/database/path"
	const requestToken = "request-only-bearer-token"
	const traceID = "0123456789abcdef0123456789abcdef"
	internal := errors.New(detail)
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	for _, tc := range []struct {
		name, method, path string
		failure            error
		bodyError          bool
		wantStatus         int
		wantCode, wantText string
	}{
		{"malformed body", "POST", "/v1/sandboxes", internal, true, 400, "invalid_request", "request body must be valid JSON"},
		{"wrapped missing sandbox", "GET", "/v1/sandboxes/box", fmt.Errorf("%s: %w", detail, store.ErrNotFound), false, 404, "not_found", "sandbox not found"},
		{"existing sandbox", "POST", "/v1/sandboxes", fmt.Errorf("%s: %w", detail, store.ErrSandboxExists), false, 409, "sandbox_exists", "sandbox ID is already in use"},
		{"suspend prefers transition conflict", "POST", "/v1/sandboxes/box/suspend", errors.Join(internal, store.ErrTransitionLocked, store.ErrNotFound), false, 409, "transition_in_progress", "a lifecycle transition is already in progress"},
		{"fork prefers stale execution conflict", "POST", "/v1/sandboxes/box/fork", errors.Join(internal, store.ErrStaleExecutionFence, store.ErrNotFound), false, 409, "stale_execution", "the execution has changed"},
		{"resume with no capacity", "POST", "/v1/sandboxes/box/resume", fmt.Errorf("%s: %w", detail, store.ErrNoIdleCarrierSlot), false, 503, "capacity_unavailable", "no Carrier capacity is currently available"},
		{"gateway resume worker unavailable", "POST", "/internal/gateway/sandboxes/box/resume", status.Error(codes.Unavailable, detail), false, 503, "worker_unavailable", "a worker is temporarily unavailable"},
		{"delete failed", "DELETE", "/v1/sandboxes/box", internal, false, 500, "internal_error", "the lifecycle request could not be completed"},
		{"template listing failed", "GET", "/v1/templates", internal, false, 500, "internal_error", "the lifecycle request could not be completed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			meta := lifecycleErrorStore{MetadataStore: store.NewMemoryMetadataStore(), err: tc.failure}
			s := NewServer(ServerConfig{Meta: meta, Catalog: store.NewMemoryRuntimeCatalog()})
			defer s.Close()
			var body io.Reader = strings.NewReader(`{}`)
			if tc.bodyError {
				body = iotest.ErrReader(tc.failure)
			}
			req := httptest.NewRequest(tc.method, tc.path+"?token="+requestToken, body)
			req.Header.Set(protocol.HeaderKontinueToken, requestToken)
			req.Header.Set("traceparent", "00-"+traceID+"-0123456789abcdef-01")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, req)
			var got struct {
				Code    string `json:"code"`
				Message string `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != tc.wantStatus || got.Code != tc.wantCode || got.Message != tc.wantText {
				t.Fatalf("unexpected response: HTTP %d %s (decode error: %v)", w.Code, w.Body.String(), err)
			}
			if strings.Contains(w.Body.String(), detail) || strings.Contains(w.Body.String(), requestToken) {
				t.Fatalf("private detail escaped in the error body: %s", w.Body.String())
			}
			if logText := logs.String(); !strings.Contains(logText, detail) || !strings.Contains(logText, traceID) || strings.Contains(logText, requestToken) {
				t.Fatalf("unexpected server log: %s", logText)
			}
		})
	}
}

func TestLifecycleFailedSandboxHidesStoredDiagnostic(t *testing.T) {
	meta := store.NewMemoryMetadataStore()
	box := &store.SandboxRecord{SandboxID: "box", State: store.SandboxStateFailed, FailureReason: "worker rejected bootstrap credential private-value"}
	if err := meta.CreateSandbox(context.Background(), box); err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{Meta: meta, Catalog: store.NewMemoryRuntimeCatalog()})
	defer s.Close()
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/sandboxes/box", nil))
	var got struct {
		State         store.SandboxState `json:"state"`
		FailureCode   string             `json:"failureCode"`
		FailureReason string             `json:"failureReason"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusOK || got.State != store.SandboxStateFailed || got.FailureCode != "sandbox_failed" || got.FailureReason != "sandbox execution failed" {
		t.Fatalf("unexpected sandbox response: HTTP %d %s (decode error: %v)", w.Code, w.Body.String(), err)
	}
	if strings.Contains(w.Body.String(), box.FailureReason) {
		t.Fatalf("stored diagnostic escaped in the response: %s", w.Body.String())
	}
}
