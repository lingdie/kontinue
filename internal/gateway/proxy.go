package gateway

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
)

type Config struct {
	TraceToken   string
	Catalog      store.RuntimeCatalog
	APIServerURL string
	BaseDomain   string
	Recorder     *telemetry.FlightRecorder
}

// Gateway is Kontinue's stateless edge reverse proxy:
// - Routes https://<port>-<sandbox-id>.<domain> and X-Kontinue-Sandbox-ID traffic directly to PodIP via Redis;
// - Blocks /internal/* control paths unconditionally;
// - Tracks active connections and debounces kt:active_at:{id} heartbeats;
// - Transparently wakes SUSPENDED sandboxes via singleflight.Group auto-resume!
type Gateway struct {
	cfg        Config
	httpClient *http.Client
	transport  http.RoundTripper
	resumeSF   singleflight.Group

	inFlightMu sync.Mutex
	inFlight   map[string]*atomic.Int64
}

func NewGateway(cfg Config) *Gateway {
	baseTr := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     60 * time.Second,
	}
	tracedTr := telemetry.NewTracingTransport("kontinue-gateway", baseTr)
	return &Gateway{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout:   45 * time.Second,
			Transport: tracedTr,
		},
		transport: tracedTr,
		inFlight:  make(map[string]*atomic.Int64),
	}
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if g.cfg.Recorder != nil {
		mux.HandleFunc(protocol.PathDebugTraces, g.cfg.Recorder.OperatorHandler(g.cfg.TraceToken))
	}
	mux.HandleFunc("/", g.handleProxy)
	return telemetry.HTTPMiddleware("kontinue-gateway", mux)
}

func (g *Gateway) ActiveConnections(sandboxID string) int64 {
	g.inFlightMu.Lock()
	c := g.inFlight[sandboxID]
	g.inFlightMu.Unlock()
	if c == nil {
		return 0
	}
	return c.Load()
}

func (g *Gateway) handleProxy(w http.ResponseWriter, r *http.Request) {
	// Security boundary: NEVER allow external ingress to reach ktd's /internal/* endpoints!
	if protocol.IsInternalControlPath(r.URL.Path) {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	sandboxID, targetPort := g.resolveTarget(r)
	if sandboxID == "" {
		http.Error(w, `{"error":"missing sandbox identifier in Host or X-Kontinue-Sandbox-ID"}`, http.StatusBadRequest)
		return
	}

	token := r.Header.Get(protocol.HeaderKontinueToken)
	if token == "" {
		http.Error(w, "sandbox credential required", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	requestID := rand.Text()
	route, err := g.cfg.Catalog.AdmitRoute(ctx, sandboxID, requestID)
	if errors.Is(err, store.ErrNotFound) {
		// Auto-Resume path: coalesce concurrent requests for this suspended sandbox into ONE wakeup flight!
		if err = g.autoResumeSingleflight(ctx, sandboxID, token); err == nil {
			route, err = g.cfg.Catalog.AdmitRoute(ctx, sandboxID, requestID)
		}
	}
	if err == nil {
		if route.AccessDigest == "" {
			// Routes published by earlier workers require a metadata lookup.
			err = g.gatewayAction(ctx, sandboxID, token, "authorize")
		} else if subtle.ConstantTimeCompare([]byte(route.AccessDigest), []byte(protocol.AccessDigest(token))) != 1 {
			err = errAccessDenied
		}
		if err != nil {
			_ = g.cfg.Catalog.FinishAdmission(ctx, sandboxID, requestID)
		}
	}
	if errors.Is(err, errAccessDenied) {
		http.Error(w, "invalid sandbox credential", http.StatusUnauthorized)
		return
	}
	if err != nil {
		w.Header().Set("Retry-After", "2")
		http.Error(w, fmt.Sprintf(`{"error":"sandbox wakeup failed: %s"}`, err.Error()), http.StatusServiceUnavailable)
		return
	}

	counter := g.counterFor(sandboxID)
	counter.Add(1)
	go g.renewAdmission(ctx, cancel, sandboxID, requestID)
	defer func() {
		counter.Add(-1)
		_ = g.cfg.Catalog.FinishAdmission(context.Background(), sandboxID, requestID)
		_ = g.cfg.Catalog.TouchSandboxActive(context.Background(), sandboxID, time.Now().UTC())
	}()
	_ = g.cfg.Catalog.TouchSandboxActive(ctx, sandboxID, time.Now().UTC())

	upstreamAddr := formatTargetHostPort(route.PodIP, targetPort)
	targetURL, err := url.Parse("http://" + upstreamAddr)
	if err != nil {
		http.Error(w, `{"error":"invalid upstream endpoint"}`, http.StatusBadGateway)
		return
	}

	proxy := &httputil.ReverseProxy{
		Director: func(outReq *http.Request) {
			outReq.URL.Scheme = targetURL.Scheme
			outReq.URL.Host = targetURL.Host
			outReq.Host = targetURL.Host
			if targetPort != protocol.DefaultKtdPort {
				outReq.Header.Del(protocol.HeaderKontinueToken)
			}
		},
		Transport: g.transport,
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, pErr error) {
			http.Error(rw, fmt.Sprintf(`{"error":"upstream sandbox unreachable: %v"}`, pErr), http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}

// Stop forwarding a long-lived stream if its gateway loses the admission lease.
func (g *Gateway) renewAdmission(ctx context.Context, cancel context.CancelFunc, id, requestID string) {
	ticker := time.NewTicker(store.AdmissionTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, done := context.WithTimeout(ctx, store.AdmissionTTL/3)
			err := g.cfg.Catalog.RenewAdmission(renewCtx, id, requestID)
			done()
			if err != nil {
				cancel()
				return
			}
		}
	}
}

func (g *Gateway) autoResumeSingleflight(callerCtx context.Context, sandboxID, token string) error {
	ctx, span := telemetry.StartSpan(callerCtx, "kontinue-gateway", "gateway.auto_resume_singleflight",
		attribute.String(telemetry.AttrSandboxID, sandboxID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	_, err, shared := g.resumeSF.Do(sandboxID+"/"+protocol.AccessDigest(token), func() (any, error) {
		// Detach from a single impatient caller cancellation so sibling callers in the flight still succeed.
		detachedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		return nil, g.gatewayAction(detachedCtx, sandboxID, token, "resume")
	})
	span.SetAttributes(attribute.Bool("singleflight.shared", shared))
	retErr = err
	return err
}

var errAccessDenied = errors.New("invalid sandbox credential")

func (g *Gateway) gatewayAction(ctx context.Context, id, token, action string) error {
	endpoint := fmt.Sprintf("%s/internal/gateway/sandboxes/%s/%s", strings.TrimRight(g.cfg.APIServerURL, "/"), url.PathEscape(id), action)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set(protocol.HeaderKontinueToken, token)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusNotFound {
		return errAccessDenied
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("sandbox %s returned status %d", action, resp.StatusCode)
	}
	return nil
}

func (g *Gateway) resolveTarget(r *http.Request) (sandboxID string, port int) {
	port = protocol.DefaultKtdPort
	if hdrID := r.Header.Get(protocol.HeaderKontinueSandboxID); hdrID != "" {
		return hdrID, port
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// Subdomain convention: <port>-<sandboxId>.<baseDomain>
	firstLabel := strings.Split(host, ".")[0]
	if dash := strings.IndexByte(firstLabel, '-'); dash > 0 {
		if p, err := strconv.Atoi(firstLabel[:dash]); err == nil && p > 0 && p <= 65535 {
			return firstLabel[dash+1:], p
		}
	}
	return "", port
}

func (g *Gateway) counterFor(sandboxID string) *atomic.Int64 {
	g.inFlightMu.Lock()
	defer g.inFlightMu.Unlock()
	c, ok := g.inFlight[sandboxID]
	if !ok {
		c = new(atomic.Int64)
		g.inFlight[sandboxID] = c
	}
	return c
}

func formatTargetHostPort(podIPOrEndpoint string, targetPort int) string {
	if strings.Contains(podIPOrEndpoint, ":") {
		if targetPort == protocol.DefaultKtdPort {
			return podIPOrEndpoint
		}
		host, _, err := net.SplitHostPort(podIPOrEndpoint)
		if err == nil {
			return net.JoinHostPort(host, strconv.Itoa(targetPort))
		}
	}
	return net.JoinHostPort(podIPOrEndpoint, strconv.Itoa(targetPort))
}
