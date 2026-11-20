package telemetry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

func TestEndToEndTracePropagationAndFlightRecorder(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "kontinue-test",
		NodeID:         "node-arm64-1",
		RingBufferSize: 128,
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	rootCtx, rootSpan := telemetry.StartSpan(ctx, "kontinue-gateway", "gateway.auto_resume",
		attribute.String(telemetry.AttrSandboxID, "sbx-m0-01"),
	)
	traceID := telemetry.TraceIDFromContext(rootCtx)
	if traceID == "" {
		t.Fatalf("expected non-empty root trace_id")
	}

	downstream := httptest.NewServer(telemetry.HTTPMiddleware("ktd", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, child := telemetry.StartSpan(r.Context(), "ktd", "ktd.held_init",
			attribute.String(telemetry.AttrSandboxID, "sbx-m0-01"),
		)
		telemetry.EndSpan(child, nil)
		w.WriteHeader(http.StatusNoContent)
	})))
	defer downstream.Close()

	client := &http.Client{Transport: telemetry.NewTracingTransport("kontinued", nil)}
	req, _ := http.NewRequestWithContext(rootCtx, http.MethodPost, downstream.URL+protocol.PathInternalInit, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_ = resp.Body.Close()
	if got := resp.Header.Get(protocol.HeaderTraceID); got != traceID {
		t.Fatalf("expected X-Trace-ID=%q, got %q", traceID, got)
	}
	telemetry.EndSpan(rootSpan, nil)

	debugSrv := httptest.NewServer(prov.Recorder().Handler())
	defer debugSrv.Close()

	qResp, err := http.Get(debugSrv.URL + "/debug/traces?sandbox_id=sbx-m0-01")
	if err != nil {
		t.Fatalf("query /debug/traces: %v", err)
	}
	defer qResp.Body.Close()

	var parsed telemetry.DebugTraceResponse
	if err := json.NewDecoder(qResp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed.Count < 2 {
		t.Fatalf("expected >= 2 spans for sbx-m0-01, got %d", parsed.Count)
	}
	for _, sp := range parsed.Spans {
		if sp.TraceID != traceID {
			t.Fatalf("trace_id mismatch on %s: got %s, want %s", sp.Name, sp.TraceID, traceID)
		}
	}
}
