package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type SpanEventItem struct {
	Name       string            `json:"name"`
	TimeUnixMs int64             `json:"timeUnixMs"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// RecordedSpan is shared identically between Go services and Rust crates (kt-protocol)
// so ktctl trace can merge spans from every component into a single causal tree.
type RecordedSpan struct {
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId,omitempty"`
	Service      string            `json:"service"`
	Name         string            `json:"name"`
	StartTime    time.Time         `json:"startTime"`
	EndTime      time.Time         `json:"endTime"`
	DurationMs   float64           `json:"durationMs"`
	Status       string            `json:"status"`
	ErrorMessage string            `json:"errorMessage,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Events       []SpanEventItem   `json:"events,omitempty"`
}

type DebugTraceResponse struct {
	Service string         `json:"service"`
	Count   int            `json:"count"`
	Spans   []RecordedSpan `json:"spans"`
}

// FlightRecorder implements sdktrace.SpanExporter over a fixed-capacity in-memory ring buffer.
type FlightRecorder struct {
	serviceName string
	capacity    int

	mu    sync.RWMutex
	ring  []RecordedSpan
	head  int
	count int
}

func NewFlightRecorder(serviceName string, capacity int) *FlightRecorder {
	if capacity <= 0 {
		capacity = 4096
	}
	return &FlightRecorder{
		serviceName: serviceName,
		capacity:    capacity,
		ring:        make([]RecordedSpan, capacity),
	}
}

// IngestExternalSpans appends spans reported by short-lived Rust CLI child processes
// (e.g. kontinue-runsc or kontinue-arena-helper) into the node daemon's FlightRecorder.
func (f *FlightRecorder) IngestExternalSpans(spans []RecordedSpan) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rec := range spans {
		f.ring[f.head] = rec
		f.head = (f.head + 1) % f.capacity
		if f.count < f.capacity {
			f.count++
		}
	}
}

func (f *FlightRecorder) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, s := range spans {
		attrs := make(map[string]string, len(s.Attributes()))
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.Emit()
		}
		var events []SpanEventItem
		for _, ev := range s.Events() {
			evAttrs := make(map[string]string, len(ev.Attributes))
			for _, kv := range ev.Attributes {
				evAttrs[string(kv.Key)] = kv.Value.Emit()
			}
			events = append(events, SpanEventItem{
				Name:       ev.Name,
				TimeUnixMs: ev.Time.UnixMilli(),
				Attributes: evAttrs,
			})
		}
		status := "OK"
		errMsg := ""
		if s.Status().Code == codes.Error {
			status = "ERROR"
			errMsg = s.Status().Description
		}
		parentID := ""
		if s.Parent().IsValid() {
			parentID = s.Parent().SpanID().String()
		}
		rec := RecordedSpan{
			TraceID:      s.SpanContext().TraceID().String(),
			SpanID:       s.SpanContext().SpanID().String(),
			ParentSpanID: parentID,
			Service:      f.serviceName,
			Name:         s.Name(),
			StartTime:    s.StartTime(),
			EndTime:      s.EndTime(),
			DurationMs:   float64(s.EndTime().Sub(s.StartTime()).Microseconds()) / 1000.0,
			Status:       status,
			ErrorMessage: errMsg,
			Attributes:   attrs,
			Events:       events,
		}
		f.ring[f.head] = rec
		f.head = (f.head + 1) % f.capacity
		if f.count < f.capacity {
			f.count++
		}
	}
	return nil
}

func (f *FlightRecorder) Shutdown(_ context.Context) error { return nil }

func (f *FlightRecorder) Query(traceID, sandboxID, snapshotID string, errorsOnly bool, limit int) []RecordedSpan {
	if limit <= 0 {
		limit = 200
	}
	f.mu.RLock()
	snapshot := make([]RecordedSpan, 0, f.count)
	start := 0
	if f.count == f.capacity {
		start = f.head
	}
	for i := 0; i < f.count; i++ {
		idx := (start + i) % f.capacity
		snapshot = append(snapshot, f.ring[idx])
	}
	f.mu.RUnlock()

	var matched []RecordedSpan
	for i := len(snapshot) - 1; i >= 0; i-- {
		sp := snapshot[i]
		if traceID != "" && sp.TraceID != traceID {
			continue
		}
		if sandboxID != "" && sp.Attributes["sandbox.id"] != sandboxID {
			continue
		}
		if snapshotID != "" && sp.Attributes["snapshot.id"] != snapshotID {
			continue
		}
		if errorsOnly && sp.Status != "ERROR" {
			continue
		}
		matched = append(matched, sp)
		if len(matched) >= limit {
			break
		}
	}
	slices.SortFunc(matched, func(a, b RecordedSpan) int {
		return a.StartTime.Compare(b.StartTime)
	})
	return matched
}

func (f *FlightRecorder) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			q := r.URL.Query()
			limit, _ := strconv.Atoi(q.Get("limit"))
			errorsOnly := q.Get("errors_only") == "true" || q.Get("errors_only") == "1"
			spans := f.Query(
				q.Get("trace_id"),
				q.Get("sandbox_id"),
				q.Get("snapshot_id"),
				errorsOnly,
				limit,
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(DebugTraceResponse{
				Service: f.serviceName,
				Count:   len(spans),
				Spans:   spans,
			})
		case http.MethodPost:
			// Allows local Rust CLI helpers (kontinue-runsc / kontinue-arena-helper) to push recorded spans.
			var batch []RecordedSpan
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			f.IngestExternalSpans(batch)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
