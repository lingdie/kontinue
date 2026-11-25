package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "trace":
		runTraceCmd(os.Args[2:])
	case "sandbox":
		runSandboxCmd(os.Args[2:])
	case "template":
		runTemplateCmd(os.Args[2:])
	case "dashboard":
		runDashboardCmd(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `ktctl - Kontinue CLI & Distributed Trace Waterfall Inspector

Usage:
  ktctl trace   [--endpoints url1,url2] [--sandbox-id id] [--trace-id hex] [--errors-only]
  ktctl sandbox [create|list|suspend|resume|connect|fork|snapshot|file-url|exec] ...
  ktctl template [create|list|get|rebuild|builds|logs|tag|delete] ...
  ktctl dashboard [--api url]
`)
}

func runDashboardCmd(args []string) {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	apiURL := fs.String("api", "http://127.0.0.1:8081", "Kontinue API Server URL")
	_ = fs.Parse(args)
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(*apiURL, "/")+"/v1/dashboard", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "request error: %v\n", err)
		os.Exit(1)
	}
	if key := os.Getenv("KONTINUE_TENANT_TOKEN"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %d | X-Trace-ID: %s\n%s\n", resp.StatusCode, resp.Header.Get(protocol.HeaderTraceID), raw)
}

func runTemplateCmd(args []string) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}
	action := args[0]
	fs := flag.NewFlagSet("template "+action, flag.ExitOnError)
	apiURL := fs.String("api", "http://127.0.0.1:8081", "Kontinue API Server URL")
	templateID := fs.String("id", "", "Template ID")
	name := fs.String("name", "", "Template name")
	baseImage := fs.String("base-image", "", "Base image")
	warmupScript := fs.String("warmup-script", "", "Warmup script")
	tier := fs.String("tier", "", "Carrier tier")
	tag := fs.String("tag", "", "Template tag")
	snapshotID := fs.String("snapshot", "", "Snapshot ID for a template tag")
	_ = fs.Parse(args[1:])
	base := strings.TrimRight(*apiURL, "/")
	var method, path string
	var body io.Reader
	switch action {
	case "create":
		raw, _ := json.Marshal(map[string]any{"name": *name, "baseImage": *baseImage, "warmupScript": *warmupScript, "tier": *tier})
		method, path, body = http.MethodPost, "/v1/templates", bytes.NewReader(raw)
	case "list":
		method, path = http.MethodGet, "/v1/templates"
	case "get":
		method, path = http.MethodGet, "/v1/templates/"+url.PathEscape(*templateID)
	case "rebuild":
		raw, _ := json.Marshal(map[string]any{"name": *name, "baseImage": *baseImage, "warmupScript": *warmupScript, "tier": *tier})
		method, path, body = http.MethodPost, "/v1/templates/"+url.PathEscape(*templateID)+"/builds", bytes.NewReader(raw)
	case "builds":
		method, path = http.MethodGet, "/v1/templates/"+url.PathEscape(*templateID)+"/builds"
	case "logs":
		method, path = http.MethodGet, "/v1/templates/"+url.PathEscape(*templateID)+"/logs"
	case "tag":
		raw, _ := json.Marshal(map[string]any{"tag": *tag, "snapshotId": *snapshotID})
		method, path, body = http.MethodPost, "/v1/templates/"+url.PathEscape(*templateID)+"/tags", bytes.NewReader(raw)
	case "delete":
		method, path = http.MethodDelete, "/v1/templates/"+url.PathEscape(*templateID)
	default:
		fmt.Fprintf(os.Stderr, "unknown template action %q\n", action)
		os.Exit(1)
	}
	req, err := http.NewRequest(method, base+path, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "request error: %v\n", err)
		os.Exit(1)
	}
	if key := os.Getenv("KONTINUE_TENANT_TOKEN"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %d | X-Trace-ID: %s\n%s\n", resp.StatusCode, resp.Header.Get(protocol.HeaderTraceID), raw)
}

func runTraceCmd(args []string) {
	fs := flag.NewFlagSet("trace", flag.ExitOnError)
	endpoints := fs.String("endpoints", "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082", "Comma-separated base URLs exposing /debug/traces")
	sandboxID := fs.String("sandbox-id", "", "Filter spans by sandbox.id")
	traceID := fs.String("trace-id", "", "Filter spans by W3C trace_id")
	errorsOnly := fs.Bool("errors-only", false, "Only show failed spans")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	spans, err := CollectAndMergeTraces(ctx, strings.Split(*endpoints, ","), *traceID, *sandboxID, *errorsOnly)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trace error: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(RenderTraceWaterfall(spans))
}

func CollectAndMergeTraces(ctx context.Context, endpoints []string, traceID, sandboxID string, errorsOnly bool) ([]telemetry.RecordedSpan, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	seen := make(map[string]telemetry.RecordedSpan)

	var failures []error
	reachable := 0
	for _, ep := range endpoints {
		ep = strings.TrimSpace(strings.TrimRight(ep, "/"))
		if ep == "" {
			continue
		}
		q := url.Values{}
		if traceID != "" {
			q.Set("trace_id", traceID)
		}
		if sandboxID != "" {
			q.Set("sandbox_id", sandboxID)
		}
		if errorsOnly {
			q.Set("errors_only", "true")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep+protocol.PathDebugTraces+"?"+q.Encode(), nil)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if key := os.Getenv("KONTINUE_TRACE_TOKEN"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := client.Do(req)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		var out telemetry.DebugTraceResponse
		if resp.StatusCode != http.StatusOK {
			failures = append(failures, fmt.Errorf("%s: HTTP %d", ep, resp.StatusCode))
		} else if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", ep, err))
		} else {
			reachable++
			for _, sp := range out.Spans {
				seen[sp.TraceID+":"+sp.SpanID] = sp
			}
		}
		_ = resp.Body.Close()
	}

	if reachable == 0 {
		return nil, fmt.Errorf("no trace endpoint available: %v", errors.Join(failures...))
	}

	merged := make([]telemetry.RecordedSpan, 0, len(seen))
	for _, sp := range seen {
		merged = append(merged, sp)
	}
	slices.SortFunc(merged, func(a, b telemetry.RecordedSpan) int {
		return a.StartTime.Compare(b.StartTime)
	})
	return merged, nil
}

// RenderTraceWaterfall formats merged Go + Rust RecordedSpans as an indented parent-child ASCII waterfall.
func RenderTraceWaterfall(spans []telemetry.RecordedSpan) string {
	if len(spans) == 0 {
		return "No matching trace spans found.\n"
	}

	children := make(map[string][]telemetry.RecordedSpan)
	hasID := make(map[string]bool, len(spans))
	for _, sp := range spans {
		hasID[sp.SpanID] = true
	}
	var roots []telemetry.RecordedSpan
	for _, sp := range spans {
		if sp.ParentSpanID == "" || !hasID[sp.ParentSpanID] {
			roots = append(roots, sp)
		} else {
			children[sp.ParentSpanID] = append(children[sp.ParentSpanID], sp)
		}
	}

	var buf strings.Builder
	fmt.Fprintf(&buf, "=== Kontinue Trace Waterfall (%d spans) ===\n", len(spans))
	var walk func(sp telemetry.RecordedSpan, depth int)
	walk = func(sp telemetry.RecordedSpan, depth int) {
		indent := strings.Repeat("  ", depth)
		prefix := "├─"
		if depth == 0 {
			prefix = "●"
		}
		statusTag := "OK"
		if sp.Status != "OK" {
			statusTag = "ERR: " + sp.ErrorMessage
		}
		var extras []string
		for _, k := range []string{telemetry.AttrSandboxID, telemetry.AttrSnapshotID, telemetry.AttrExecutionID, telemetry.AttrArenaTier, telemetry.AttrChunkSource} {
			if v := sp.Attributes[k]; v != "" {
				extras = append(extras, fmt.Sprintf("%s=%s", k, v))
			}
		}
		meta := ""
		if len(extras) > 0 {
			meta = " {" + strings.Join(extras, ", ") + "}"
		}
		fmt.Fprintf(&buf, "%s%s [%s] %s (%.2fms) [%s]%s (trace=%s span=%s)\n",
			indent, prefix, sp.Service, sp.Name, sp.DurationMs, statusTag, meta, sp.TraceID, sp.SpanID)
		for _, ch := range children[sp.SpanID] {
			walk(ch, depth+1)
		}
	}
	for _, r := range roots {
		walk(r, 0)
	}
	return buf.String()
}

func runSandboxCmd(args []string) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}
	action := args[0]
	fs := flag.NewFlagSet("sandbox "+action, flag.ExitOnError)
	apiURL := fs.String("api", "http://127.0.0.1:8081", "Kontinue API Server URL")
	gatewayURL := fs.String("gateway", "http://127.0.0.1:8080", "Kontinue Gateway URL")
	templateID := fs.String("template", "", "Template ID")
	sandboxID := fs.String("id", "", "Sandbox ID")
	token := fs.String("token", "", "Sandbox access token")
	cmdStr := fs.String("cmd", "uname -a", "Shell command for exec")
	filePath := fs.String("path", "", "Absolute guest file path")
	fileOperation := fs.String("operation", "read", "File URL operation: read or write")
	fileExpiry := fs.Int64("expires-in", 300, "Signed file URL lifetime in seconds")
	_ = fs.Parse(args[1:])

	client := &http.Client{Timeout: 30 * time.Second}
	var req *http.Request
	var err error

	switch action {
	case "create":
		body, _ := json.Marshal(map[string]any{"templateId": *templateID})
		req, err = http.NewRequest(http.MethodPost, strings.TrimRight(*apiURL, "/")+"/v1/sandboxes", bytes.NewReader(body))
	case "suspend":
		req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sandboxes/%s/suspend", strings.TrimRight(*apiURL, "/"), *sandboxID), nil)
	case "resume":
		req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sandboxes/%s/resume", strings.TrimRight(*apiURL, "/"), *sandboxID), nil)
	case "fork":
		body, _ := json.Marshal(map[string]any{"count": 2})
		req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sandboxes/%s/fork", strings.TrimRight(*apiURL, "/"), *sandboxID), bytes.NewReader(body))
	case "exec":
		body, _ := json.Marshal(protocol.ExecRequest{Command: []string{"/bin/sh", "-c", *cmdStr}})
		req, err = http.NewRequest(http.MethodPost, strings.TrimRight(*gatewayURL, "/")+protocol.PathProcessExec, bytes.NewReader(body))
		if req != nil {
			req.Header.Set(protocol.HeaderKontinueSandboxID, *sandboxID)
			req.Header.Set(protocol.HeaderKontinueToken, *token)
		}
	case "list":
		req, err = http.NewRequest(http.MethodGet, strings.TrimRight(*apiURL, "/")+"/v1/sandboxes?limit=100", nil)
	case "connect":
		req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sandboxes/%s/connect", strings.TrimRight(*apiURL, "/"), *sandboxID), nil)
	case "snapshot":
		req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sandboxes/%s/snapshots", strings.TrimRight(*apiURL, "/"), *sandboxID), nil)
	case "file-url":
		body, _ := json.Marshal(map[string]any{"path": *filePath, "operation": *fileOperation, "expiresInSeconds": *fileExpiry})
		req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sandboxes/%s/files/urls", strings.TrimRight(*apiURL, "/"), *sandboxID), bytes.NewReader(body))
	default:
		fmt.Fprintf(os.Stderr, "unknown sandbox action %q\n", action)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "request error: %v\n", err)
		os.Exit(1)
	}
	if key := os.Getenv("KONTINUE_TENANT_TOKEN"); action != "exec" && key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %d | X-Trace-ID: %s\n%s\n", resp.StatusCode, resp.Header.Get(protocol.HeaderTraceID), string(raw))
}
