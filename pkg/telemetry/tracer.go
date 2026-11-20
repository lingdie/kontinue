package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	AttrSandboxID   = "sandbox.id"
	AttrTemplateID  = "template.id"
	AttrSnapshotID  = "snapshot.id"
	AttrExecutionID = "execution.id"
	AttrSlotID      = "carrier.slot_id"
	AttrNodeID      = "node.id"
	AttrArenaTier   = "arena.tier"
	AttrChunkIndex  = "chunk.index"
	AttrChunkHash   = "chunk.blake3"
	AttrChunkSource = "chunk.source"
	AttrBytes       = "kontinue.bytes"
	AttrRestoreMode = "restore.mode"
)

type Config struct {
	ServiceName    string
	NodeID         string
	OTLPEndpoint   string
	RingBufferSize int
}

type Provider struct {
	tp       *sdktrace.TracerProvider
	recorder *FlightRecorder
}

var (
	defaultRecorderMu sync.RWMutex
	defaultRecorder   = NewFlightRecorder("kontinue", 4096)
	propagatorOnce    sync.Once
)

func init() {
	ensurePropagator()
	protocol.DefaultHTTPTransportWrapper = NewTracingTransport
}

func ensurePropagator() {
	propagatorOnce.Do(func() {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	})
}

func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	ensurePropagator()
	if cfg.ServiceName == "" {
		cfg.ServiceName = "kontinue"
	}
	if cfg.OTLPEndpoint == "" {
		cfg.OTLPEndpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	recorder := NewFlightRecorder(cfg.ServiceName, cfg.RingBufferSize)
	defaultRecorderMu.Lock()
	defaultRecorder = recorder
	defaultRecorderMu.Unlock()

	attrs := []attribute.KeyValue{semconv.ServiceName(cfg.ServiceName)}
	if cfg.NodeID != "" {
		attrs = append(attrs, attribute.String(AttrNodeID, cfg.NodeID))
	}
	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		return nil, err
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(recorder)),
	}

	if cfg.OTLPEndpoint != "" {
		ep := strings.TrimPrefix(strings.TrimPrefix(cfg.OTLPEndpoint, "http://"), "https://")
		otlpOpts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(ep)}
		if !strings.HasPrefix(cfg.OTLPEndpoint, "https://") {
			otlpOpts = append(otlpOpts, otlptracehttp.WithInsecure())
		}
		if exp, expErr := otlptracehttp.New(ctx, otlpOpts...); expErr == nil {
			opts = append(opts, sdktrace.WithBatcher(exp))
		}
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return &Provider{tp: tp, recorder: recorder}, nil
}

func (p *Provider) Recorder() *FlightRecorder { return p.recorder }

func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.tp == nil {
		return nil
	}
	return p.tp.Shutdown(ctx)
}

func DefaultRecorder() *FlightRecorder {
	defaultRecorderMu.RLock()
	defer defaultRecorderMu.RUnlock()
	return defaultRecorder
}

func StartSpan(ctx context.Context, component, name string, kv ...attribute.KeyValue) (context.Context, trace.Span) {
	ensurePropagator()
	return otel.Tracer(component).Start(ctx, name, trace.WithAttributes(kv...))
}

func EndSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

func Logger(ctx context.Context, component string) *slog.Logger {
	base := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("component", component)
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return base.With(
			"trace_id", sc.TraceID().String(),
			"span_id", sc.SpanID().String(),
		)
	}
	return base
}

func InjectEnv(ctx context.Context, env []string) []string {
	ensurePropagator()
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp := carrier.Get("traceparent"); tp != "" {
		env = append(env, "TRACEPARENT="+tp)
	}
	if ts := carrier.Get("tracestate"); ts != "" {
		env = append(env, "TRACESTATE="+ts)
	}
	return env
}

func InjectAnnotations(ctx context.Context, annotations map[string]string) map[string]string {
	ensurePropagator()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp := carrier.Get("traceparent"); tp != "" {
		annotations[protocol.AnnotationTraceParent] = tp
	}
	return annotations
}

func ExtractAnnotations(ctx context.Context, annotations map[string]string) context.Context {
	ensurePropagator()
	if len(annotations) == 0 {
		return ctx
	}
	tp := annotations[protocol.AnnotationTraceParent]
	if tp == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func HTTPMiddleware(component string, next http.Handler) http.Handler {
	ensurePropagator()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == protocol.PathDebugTraces {
			next.ServeHTTP(w, r)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		attrs := []attribute.KeyValue{
			attribute.String("http.method", r.Method),
			attribute.String("http.path", r.URL.Path),
		}
		if sbxID := r.Header.Get(protocol.HeaderKontinueSandboxID); sbxID != "" {
			attrs = append(attrs, attribute.String(AttrSandboxID, sbxID))
		}
		ctx, span := otel.Tracer(component).Start(ctx, r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		if tid := TraceIDFromContext(ctx); tid != "" {
			w.Header().Set(protocol.HeaderTraceID, tid)
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		span.SetAttributes(attribute.Int("http.status_code", rec.status))
		if rec.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		} else {
			span.SetStatus(codes.Ok, "")
		}
	})
}

type tracingRoundTripper struct {
	component string
	base      http.RoundTripper
}

func NewTracingTransport(component string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &tracingRoundTripper{component: component, base: base}
}

func (t *tracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ensurePropagator()
	ctx, span := otel.Tracer(t.component).Start(req.Context(), "HTTP "+req.Method+" "+req.URL.Path,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.method", req.Method),
			attribute.String("http.url", req.URL.Redacted()),
		),
	)
	defer span.End()

	cloned := req.Clone(ctx)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(cloned.Header))
	resp, err := t.base.RoundTrip(cloned)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
	if resp.StatusCode >= 500 {
		span.SetStatus(codes.Error, resp.Status)
	} else {
		span.SetStatus(codes.Ok, "")
	}
	return resp, nil
}

type grpcMetadataCarrier metadata.MD

func (m grpcMetadataCarrier) Get(key string) string {
	vals := metadata.MD(m).Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func (m grpcMetadataCarrier) Set(key, val string) { metadata.MD(m).Set(key, val) }
func (m grpcMetadataCarrier) Keys() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func UnaryServerInterceptor(component string) grpc.UnaryServerInterceptor {
	ensurePropagator()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = otel.GetTextMapPropagator().Extract(ctx, grpcMetadataCarrier(md))
		}
		ctx, span := otel.Tracer(component).Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
		resp, err := handler(ctx, req)
		EndSpan(span, err)
		return resp, err
	}
}

func UnaryClientInterceptor(component string) grpc.UnaryClientInterceptor {
	ensurePropagator()
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := otel.Tracer(component).Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient))
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}
		otel.GetTextMapPropagator().Inject(ctx, grpcMetadataCarrier(md))
		ctx = metadata.NewOutgoingContext(ctx, md)
		err := invoker(ctx, method, req, reply, cc, opts...)
		EndSpan(span, err)
		return err
	}
}

type tracedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *tracedServerStream) Context() context.Context { return s.ctx }

func StreamServerInterceptor(component string) grpc.StreamServerInterceptor {
	ensurePropagator()
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = otel.GetTextMapPropagator().Extract(ctx, grpcMetadataCarrier(md))
		}
		ctx, span := otel.Tracer(component).Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
		err := handler(srv, &tracedServerStream{ServerStream: ss, ctx: ctx})
		EndSpan(span, err)
		return err
	}
}

func StreamClientInterceptor(component string) grpc.StreamClientInterceptor {
	ensurePropagator()
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, span := otel.Tracer(component).Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient))
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}
		otel.GetTextMapPropagator().Inject(ctx, grpcMetadataCarrier(md))
		ctx = metadata.NewOutgoingContext(ctx, md)
		cs, err := streamer(ctx, desc, cc, method, opts...)
		EndSpan(span, err)
		return cs, err
	}
}
