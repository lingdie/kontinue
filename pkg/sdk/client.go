// Package sdk provides a small typed Go client for the public Kontinue API.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type Client struct {
	BaseURL     string
	GatewayURL  string
	TenantToken string
	HTTPClient  *http.Client
	MaxRetries  int
}

type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("kontinue API %d %s: %s", e.Status, e.Code, e.Message)
}

type Sandbox struct {
	SandboxID          string                 `json:"sandboxId"`
	TemplateID         string                 `json:"templateId,omitempty"`
	SnapshotID         string                 `json:"activeSnapshotId,omitempty"`
	BaseImage          string                 `json:"baseImage"`
	Tier               string                 `json:"tier"`
	State              string                 `json:"state"`
	ExecutionID        int64                  `json:"executionId"`
	LifecycleID        string                 `json:"lifecycleId"`
	IdleTimeoutSeconds int64                  `json:"idleTimeoutSeconds"`
	Metadata           map[string]string      `json:"metadata,omitempty"`
	Volumes            []protocol.VolumeMount `json:"volumes,omitempty"`
	AccessToken        string                 `json:"accessToken,omitempty"`
	client             *Client                `json:"-"`
	Commands           *Commands              `json:"-"`
	Files              *Filesystem            `json:"-"`
	PTY                *PTY                   `json:"-"`
}

type SandboxPage struct {
	Sandboxes  []Sandbox `json:"sandboxes"`
	NextCursor string    `json:"nextCursor"`
}

type Template struct {
	TemplateID       string            `json:"templateId"`
	Name             string            `json:"name"`
	BaseImage        string            `json:"baseImage"`
	Tier             string            `json:"tier"`
	Version          int               `json:"version"`
	Tags             map[string]string `json:"tags,omitempty"`
	Phase            string            `json:"phase"`
	GoldenSnapshotID string            `json:"goldenSnapshotId,omitempty"`
}

type TemplateBuild struct {
	Version       int      `json:"version"`
	SnapshotID    string   `json:"snapshotId,omitempty"`
	Phase         string   `json:"phase"`
	StatusMessage string   `json:"statusMessage,omitempty"`
	Logs          []string `json:"logs,omitempty"`
}

type Volume struct {
	VolumeID string            `json:"volumeId"`
	Name     string            `json:"name,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Snapshot struct {
	SnapshotID string `json:"snapshotId"`
	Kind       string `json:"kind"`
	BaseImage  string `json:"baseImage"`
	Superseded bool   `json:"superseded"`
}

type SignedFileURL struct {
	URL       string `json:"url"`
	Path      string `json:"path"`
	Operation string `json:"operation"`
	ExpiresAt int64  `json:"expiresAtUnixSeconds"`
}

type Webhook struct {
	WebhookID string    `json:"webhookId"`
	URL       string    `json:"url"`
	Events    []string  `json:"events,omitempty"`
	Secret    string    `json:"secret,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type DashboardSummary struct {
	GeneratedAt   time.Time      `json:"generatedAt"`
	SandboxCounts map[string]int `json:"sandboxCounts"`
	Sandboxes     []Sandbox      `json:"sandboxes"`
	Templates     []Template     `json:"templates"`
	Volumes       []Volume       `json:"volumes"`
}

type ForkChildOptions struct {
	SandboxID          string                 `json:"sandboxId,omitempty"`
	EnvVars            map[string]string      `json:"envVars,omitempty"`
	Metadata           map[string]string      `json:"metadata,omitempty"`
	IdleTimeoutSeconds *int64                 `json:"idleTimeoutSeconds,omitempty"`
	CPUMillis          *int64                 `json:"cpuMillis,omitempty"`
	MemoryBytes        *int64                 `json:"memoryBytes,omitempty"`
	Volumes            []protocol.VolumeMount `json:"volumes,omitempty"`
}

type ForkRequest struct {
	Count    int                `json:"count,omitempty"`
	EnvVars  map[string]string  `json:"envVars,omitempty"`
	Children []ForkChildOptions `json:"children,omitempty"`
}

type ForkChildResult struct {
	ChildID string   `json:"childId"`
	Sandbox *Sandbox `json:"sandbox,omitempty"`
	Error   string   `json:"error,omitempty"`
}

func NewClient(baseURL, tenantToken string) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	return &Client{BaseURL: baseURL, GatewayURL: baseURL, TenantToken: tenantToken, HTTPClient: &http.Client{Timeout: 30 * time.Second}, MaxRetries: 3}
}

type ClientOptions struct {
	BaseURL     string
	GatewayURL  string
	TenantToken string
	HTTPClient  *http.Client
	MaxRetries  int
}

func NewClientWithOptions(options ClientOptions) *Client {
	baseURL := strings.TrimRight(options.BaseURL, "/")
	gatewayURL := strings.TrimRight(options.GatewayURL, "/")
	if gatewayURL == "" {
		gatewayURL = baseURL
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	maxRetries := options.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	return &Client{BaseURL: baseURL, GatewayURL: gatewayURL, TenantToken: options.TenantToken, HTTPClient: httpClient, MaxRetries: maxRetries}
}

func (c *Client) attachSandbox(sandbox *Sandbox) *Sandbox {
	if sandbox == nil {
		return nil
	}
	sandbox.client = c
	sandbox.Commands = &Commands{client: c, sandbox: sandbox}
	sandbox.Files = &Filesystem{client: c, sandbox: sandbox}
	sandbox.PTY = &PTY{client: c, sandbox: sandbox}
	return sandbox
}

func (c *Client) doJSON(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	var err error
	if in != nil {
		body, err = json.Marshal(in)
		if err != nil {
			return err
		}
	}
	maxRetries := c.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.TenantToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.TenantToken)
		}
		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			if attempt < maxRetries && ctx.Err() == nil {
				if err := waitRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return err
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 500 && attempt < maxRetries {
			if err := waitRetry(ctx, attempt); err != nil {
				return err
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			var problem struct {
				Code    string `json:"code"`
				Message string `json:"error"`
			}
			_ = json.Unmarshal(raw, &problem)
			return &APIError{Status: resp.StatusCode, Code: problem.Code, Message: problem.Message}
		}
		if out != nil && len(raw) > 0 {
			return json.Unmarshal(raw, out)
		}
		return nil
	}
}

func waitRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(1<<min(attempt, 5)) * 100 * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) CreateSandbox(ctx context.Context, request any) (*Sandbox, error) {
	var out Sandbox
	err := c.doJSON(ctx, http.MethodPost, "/v1/sandboxes", request, &out)
	return c.attachSandbox(&out), err
}
func (c *Client) ListSandboxes(ctx context.Context, state, templateID, cursor string, limit int) (*SandboxPage, error) {
	q := "?limit=" + strconv.Itoa(limit)
	if state != "" {
		q += "&state=" + urlQueryEscape(state)
	}
	if templateID != "" {
		q += "&templateId=" + urlQueryEscape(templateID)
	}
	if cursor != "" {
		q += "&cursor=" + urlQueryEscape(cursor)
	}
	var out SandboxPage
	return &out, c.doJSON(ctx, http.MethodGet, "/v1/sandboxes"+q, nil, &out)
}
func (c *Client) Connect(ctx context.Context, sandboxID string) (*Sandbox, error) {
	var out Sandbox
	err := c.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(sandboxID)+"/connect", nil, &out)
	return c.attachSandbox(&out), err
}
func (c *Client) Suspend(ctx context.Context, sandboxID string) (*Snapshot, error) {
	var out Snapshot
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(sandboxID)+"/suspend", nil, &out)
}
func (c *Client) Resume(ctx context.Context, sandboxID string) (*Sandbox, error) {
	var out Sandbox
	err := c.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(sandboxID)+"/resume", nil, &out)
	return c.attachSandbox(&out), err
}
func (c *Client) CreateSnapshot(ctx context.Context, sandboxID string) (*Snapshot, error) {
	var out Snapshot
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(sandboxID)+"/snapshots", nil, &out)
}
func (c *Client) Fork(ctx context.Context, sandboxID string, request ForkRequest) ([]ForkChildResult, error) {
	var raw json.RawMessage
	path := "/v1/sandboxes/" + urlQueryEscape(sandboxID) + "/fork"
	if err := c.doJSON(ctx, http.MethodPost, path, request, &raw); err != nil {
		return nil, err
	}
	var results []ForkChildResult
	if err := json.Unmarshal(raw, &results); err == nil && (len(results) == 0 || results[0].ChildID != "") {
		for i := range results {
			c.attachSandbox(results[i].Sandbox)
		}
		return results, nil
	}
	var sandboxes []Sandbox
	if err := json.Unmarshal(raw, &sandboxes); err != nil {
		return nil, err
	}
	results = make([]ForkChildResult, 0, len(sandboxes))
	for i := range sandboxes {
		c.attachSandbox(&sandboxes[i])
		results = append(results, ForkChildResult{ChildID: sandboxes[i].SandboxID, Sandbox: &sandboxes[i]})
	}
	return results, nil
}
func (c *Client) CreateSignedFileURL(ctx context.Context, sandboxID, path, operation string, expiresInSeconds int64) (*SignedFileURL, error) {
	var out SignedFileURL
	request := struct {
		Path             string `json:"path"`
		Operation        string `json:"operation"`
		ExpiresInSeconds int64  `json:"expiresInSeconds,omitempty"`
	}{Path: path, Operation: operation, ExpiresInSeconds: expiresInSeconds}
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(sandboxID)+"/files/urls", request, &out)
}
func (c *Client) CreateVolume(ctx context.Context, request any) (*Volume, error) {
	var out Volume
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/volumes", request, &out)
}
func (c *Client) ListVolumes(ctx context.Context) ([]Volume, error) {
	var out []Volume
	err := c.doJSON(ctx, http.MethodGet, "/v1/volumes", nil, &out)
	return out, err
}
func (c *Client) ListTemplates(ctx context.Context) ([]Template, error) {
	var out []Template
	err := c.doJSON(ctx, http.MethodGet, "/v1/templates", nil, &out)
	return out, err
}
func (c *Client) CreateTemplate(ctx context.Context, request any) (*Template, error) {
	var out Template
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/templates", request, &out)
}
func (c *Client) GetTemplate(ctx context.Context, templateID string) (*Template, error) {
	var out Template
	return &out, c.doJSON(ctx, http.MethodGet, "/v1/templates/"+urlQueryEscape(templateID), nil, &out)
}
func (c *Client) RebuildTemplate(ctx context.Context, templateID string, request any) (*Template, error) {
	var out Template
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/templates/"+urlQueryEscape(templateID)+"/builds", request, &out)
}
func (c *Client) TemplateBuilds(ctx context.Context, templateID string) ([]TemplateBuild, error) {
	var out []TemplateBuild
	err := c.doJSON(ctx, http.MethodGet, "/v1/templates/"+urlQueryEscape(templateID)+"/builds", nil, &out)
	return out, err
}
func (c *Client) TemplateLogs(ctx context.Context, templateID string) (map[string]any, error) {
	var out map[string]any
	err := c.doJSON(ctx, http.MethodGet, "/v1/templates/"+urlQueryEscape(templateID)+"/logs", nil, &out)
	return out, err
}
func (c *Client) TagTemplate(ctx context.Context, templateID, tag, snapshotID string) (*Template, error) {
	var out Template
	request := struct {
		Tag        string `json:"tag"`
		SnapshotID string `json:"snapshotId,omitempty"`
	}{Tag: tag, SnapshotID: snapshotID}
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/templates/"+urlQueryEscape(templateID)+"/tags", request, &out)
}
func (c *Client) DeleteTemplate(ctx context.Context, templateID string) error {
	return c.doJSON(ctx, http.MethodDelete, "/v1/templates/"+urlQueryEscape(templateID), nil, nil)
}

func (c *Client) CreateWebhook(ctx context.Context, request any) (*Webhook, error) {
	var out Webhook
	return &out, c.doJSON(ctx, http.MethodPost, "/v1/webhooks", request, &out)
}
func (c *Client) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	var out []Webhook
	err := c.doJSON(ctx, http.MethodGet, "/v1/webhooks", nil, &out)
	return out, err
}
func (c *Client) GetWebhook(ctx context.Context, webhookID string) (*Webhook, error) {
	var out Webhook
	return &out, c.doJSON(ctx, http.MethodGet, "/v1/webhooks/"+urlQueryEscape(webhookID), nil, &out)
}
func (c *Client) DeleteWebhook(ctx context.Context, webhookID string) error {
	return c.doJSON(ctx, http.MethodDelete, "/v1/webhooks/"+urlQueryEscape(webhookID), nil, nil)
}

func (c *Client) Dashboard(ctx context.Context) (*DashboardSummary, error) {
	var out DashboardSummary
	return &out, c.doJSON(ctx, http.MethodGet, "/v1/dashboard", nil, &out)
}

func urlQueryEscape(value string) string {
	return strings.NewReplacer("%", "%25", " ", "%20", "?", "%3F", "&", "%26", "/", "%2F").Replace(value)
}
