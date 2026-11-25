package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"golang.org/x/net/websocket"
)

type SandboxCreateOptions struct {
	TemplateID                  string                 `json:"templateId,omitempty"`
	SnapshotID                  string                 `json:"snapshotId,omitempty"`
	BaseImage                   string                 `json:"baseImage,omitempty"`
	Tier                        string                 `json:"tier,omitempty"`
	Timeout                     time.Duration          `json:"-"`
	IdleTimeoutSeconds          int64                  `json:"idleTimeoutSeconds,omitempty"`
	EnvVars                     map[string]string      `json:"envVars,omitempty"`
	Metadata                    map[string]string      `json:"metadata,omitempty"`
	CPUMillis                   int64                  `json:"cpuMillis,omitempty"`
	MemoryBytes                 int64                  `json:"memoryBytes,omitempty"`
	AllowFilesystemOnlyFallback bool                   `json:"allowFilesystemOnlyFallback,omitempty"`
	Volumes                     []protocol.VolumeMount `json:"volumes,omitempty"`
}

type CommandRunOptions struct {
	Background bool
	CWD        string
	Env        map[string]string
	Stdin      string
	Timeout    time.Duration
	OnStdout   func(string)
	OnStderr   func(string)
}

type CommandResult struct {
	PID      int
	Stdout   string
	Stderr   string
	ExitCode int
	Error    string
	TimedOut bool
	Duration time.Duration
	Running  bool
}

type CommandExitError struct {
	Result *CommandResult
}

func (e *CommandExitError) Error() string {
	return fmt.Sprintf("command exited with code %d", e.Result.ExitCode)
}

type CommandHandle struct {
	commands *Commands
	PID      int
}

func (h *CommandHandle) Wait(ctx context.Context) (*CommandResult, error) {
	result, err := h.commands.Wait(ctx, h.PID)
	if err != nil {
		return nil, err
	}
	if result.ExitCode != 0 {
		return result, &CommandExitError{Result: result}
	}
	return result, nil
}

func (h *CommandHandle) Kill(ctx context.Context) error {
	return h.commands.Kill(ctx, h.PID)
}

type Commands struct {
	client  *Client
	sandbox *Sandbox
}

func (c *Commands) Run(ctx context.Context, command string, options CommandRunOptions) (*CommandResult, error) {
	streamOutput := !options.Background && (options.OnStdout != nil || options.OnStderr != nil)
	timeoutMs := int64(0)
	if options.Timeout > 0 {
		timeoutMs = options.Timeout.Milliseconds()
		if timeoutMs == 0 {
			timeoutMs = 1
		}
	}
	request := protocol.ExecRequest{
		Command:    []string{"/bin/sh", "-c", command},
		WorkingDir: options.CWD,
		Env:        options.Env,
		Stdin:      options.Stdin,
		TimeoutMs:  timeoutMs,
		Background: options.Background || streamOutput,
	}
	var response protocol.ExecResponse
	if err := c.client.sandboxJSON(ctx, c.sandbox, http.MethodPost, protocol.PathProcessExec, request, &response); err != nil {
		return nil, err
	}
	result := commandResult(response)
	if streamOutput {
		streamed, err := c.stream(ctx, result, options)
		if err != nil {
			return nil, err
		}
		if streamed.ExitCode != 0 {
			return streamed, &CommandExitError{Result: streamed}
		}
		return streamed, nil
	}
	if !options.Background && result.ExitCode != 0 {
		return &result, &CommandExitError{Result: &result}
	}
	return &result, nil
}

func (c *Commands) stream(ctx context.Context, initial CommandResult, options CommandRunOptions) (*CommandResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.client.GatewayURL+protocol.PathProcessLogStream+"?pid="+url.QueryEscape(fmt.Sprint(initial.PID)), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Kontinue-Sandbox-ID", c.sandbox.SandboxID)
	req.Header.Set("X-Kontinue-Token", c.sandbox.AccessToken)
	resp, err := c.client.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sandbox process stream status %d", resp.StatusCode)
	}
	result := initial
	result.Stdout = ""
	result.Stderr = ""
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	for scanner.Scan() {
		var event struct {
			Type     string `json:"type"`
			Data     string `json:"data"`
			ExitCode int    `json:"exitCode"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		switch event.Type {
		case "stdout":
			result.Stdout += event.Data
			if options.OnStdout != nil {
				options.OnStdout(event.Data)
			}
		case "stderr":
			result.Stderr += event.Data
			if options.OnStderr != nil {
				options.OnStderr(event.Data)
			}
		case "exit":
			result.ExitCode = event.ExitCode
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Commands) Connect(pid int) *CommandHandle {
	return &CommandHandle{commands: c, PID: pid}
}

func (c *Commands) List(ctx context.Context) ([]protocol.ProcessInfo, error) {
	var out []protocol.ProcessInfo
	return out, c.client.sandboxJSON(ctx, c.sandbox, http.MethodGet, protocol.PathProcessList, nil, &out)
}

func (c *Commands) Kill(ctx context.Context, pid int) error {
	return c.client.sandboxJSON(ctx, c.sandbox, http.MethodPost, protocol.PathProcessKill+"?pid="+url.QueryEscape(fmt.Sprint(pid)), nil, nil)
}

func (c *Commands) Wait(ctx context.Context, pid int) (*CommandResult, error) {
	for {
		var response protocol.ProcessLogsResponse
		if err := c.client.sandboxJSON(ctx, c.sandbox, http.MethodGet, protocol.PathProcessLogs+"?pid="+url.QueryEscape(fmt.Sprint(pid)), nil, &response); err != nil {
			return nil, err
		}
		result := CommandResult{PID: response.PID, Stdout: response.Stdout, Stderr: response.Stderr, ExitCode: response.ExitCode, Running: response.Running}
		if !response.Running {
			return &result, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

type Filesystem struct {
	client  *Client
	sandbox *Sandbox
}

func (f *Filesystem) Read(ctx context.Context, path string) ([]byte, error) {
	return f.client.sandboxBytes(ctx, f.sandbox, http.MethodGet, protocol.PathFiles+"?path="+url.QueryEscape(path), nil, "")
}

func (f *Filesystem) Write(ctx context.Context, path string, content []byte) error {
	_, err := f.client.sandboxBytes(ctx, f.sandbox, http.MethodPut, protocol.PathFilesStream+"?path="+url.QueryEscape(path), bytes.NewReader(content), "application/octet-stream")
	return err
}

func (f *Filesystem) DownloadURL(ctx context.Context, path string, expiresInSeconds int64) (string, error) {
	result, err := f.client.CreateSignedFileURL(ctx, f.sandbox.SandboxID, path, "download", expiresInSeconds)
	if err != nil {
		return "", err
	}
	return result.URL, nil
}

func (f *Filesystem) UploadURL(ctx context.Context, path string, expiresInSeconds int64) (string, error) {
	result, err := f.client.CreateSignedFileURL(ctx, f.sandbox.SandboxID, path, "upload", expiresInSeconds)
	if err != nil {
		return "", err
	}
	return result.URL, nil
}

func (f *Filesystem) List(ctx context.Context, path string) ([]protocol.FileEntry, error) {
	var out []protocol.FileEntry
	err := f.client.sandboxJSON(ctx, f.sandbox, http.MethodGet, protocol.PathFilesList+"?path="+url.QueryEscape(path), nil, &out)
	return out, err
}

func (f *Filesystem) Stat(ctx context.Context, path string) (*protocol.FileInfo, error) {
	var out protocol.FileInfo
	err := f.client.sandboxJSON(ctx, f.sandbox, http.MethodGet, protocol.PathFilesInfo+"?path="+url.QueryEscape(path), nil, &out)
	return &out, err
}

func (f *Filesystem) Exists(ctx context.Context, path string) (bool, error) {
	_, err := f.Stat(ctx, path)
	if err == nil {
		return true, nil
	}
	if apiErr, ok := err.(*APIError); ok && apiErr.Status == http.StatusNotFound {
		return false, nil
	}
	return false, err
}

func (f *Filesystem) MakeDir(ctx context.Context, path string) error {
	return f.client.sandboxJSON(ctx, f.sandbox, http.MethodPost, protocol.PathFilesMkdir+"?path="+url.QueryEscape(path), nil, nil)
}

func (f *Filesystem) Remove(ctx context.Context, path string) error {
	return f.client.sandboxJSON(ctx, f.sandbox, http.MethodDelete, protocol.PathFiles+"?path="+url.QueryEscape(path), nil, nil)
}

func (f *Filesystem) Rename(ctx context.Context, oldPath, newPath string) error {
	return f.client.sandboxJSON(ctx, f.sandbox, http.MethodPost, protocol.PathFilesRename, protocol.FileRenameRequest{From: oldPath, To: newPath}, nil)
}

type PTY struct {
	client   *Client
	sandbox  *Sandbox
	mu       sync.Mutex
	sessions map[int]*PtySession
}

type PtyCreateOptions struct {
	Cmd    string
	Args   []string
	Envs   map[string]string
	CWD    string
	Cols   int
	Rows   int
	OnData func([]byte)
	OnExit func(int)
}

type PtySession struct {
	conn      *websocket.Conn
	PID       int
	done      chan struct{}
	ready     chan error
	closeOnce sync.Once
	writeMu   sync.Mutex
	options   PtyCreateOptions
}

func (p *PTY) Create(ctx context.Context, options PtyCreateOptions) (*PtySession, error) {
	return p.open(ctx, options, 0)
}

func (p *PTY) open(ctx context.Context, options PtyCreateOptions, pid int) (*PtySession, error) {
	if options.Cmd == "" {
		options.Cmd = "/bin/sh"
	}
	if options.Cols == 0 {
		options.Cols = 80
	}
	if options.Rows == 0 {
		options.Rows = 24
	}
	params := url.Values{}
	if pid > 0 {
		params.Set("pid", fmt.Sprint(pid))
	} else {
		params.Set("cmd", options.Cmd)
		args, err := json.Marshal(options.Args)
		if err != nil {
			return nil, err
		}
		params.Set("args", string(args))
		params.Set("cols", fmt.Sprint(options.Cols))
		params.Set("rows", fmt.Sprint(options.Rows))
		if options.CWD != "" {
			params.Set("cwd", options.CWD)
		}
		if len(options.Envs) > 0 {
			envs, err := json.Marshal(options.Envs)
			if err != nil {
				return nil, err
			}
			params.Set("envs", string(envs))
		}
	}
	base := p.client.GatewayURL
	parsed, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	origScheme := parsed.Scheme
	parsed.Scheme = map[string]string{"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}[origScheme]
	if parsed.Scheme == "" {
		return nil, fmt.Errorf("unsupported Gateway URL scheme %q", origScheme)
	}
	parsed.Path = "/v1/pty"
	parsed.RawQuery = params.Encode()
	config, err := websocket.NewConfig(parsed.String(), base)
	if err != nil {
		return nil, err
	}
	config.Header.Set("X-Kontinue-Sandbox-ID", p.sandbox.SandboxID)
	config.Header.Set("X-Kontinue-Token", p.sandbox.AccessToken)
	config.Protocol = []string{"kontinue"}
	conn, err := websocket.DialConfig(config)
	if err != nil {
		return nil, err
	}
	session := &PtySession{conn: conn, done: make(chan struct{}), ready: make(chan error, 1), options: options}
	go session.readLoop()
	select {
	case err := <-session.ready:
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	}
	p.mu.Lock()
	if p.sessions == nil {
		p.sessions = make(map[int]*PtySession)
	}
	p.sessions[session.PID] = session
	p.mu.Unlock()
	return session, nil
}

func (p *PtySession) readLoop() {
	ready := false
	defer func() {
		if !ready {
			p.ready <- fmt.Errorf("PTY WebSocket closed before ready")
		}
		p.closeOnce.Do(func() { close(p.done) })
	}()
	for {
		var payload []byte
		if err := websocket.Message.Receive(p.conn, &payload); err != nil {
			return
		}
		var control struct {
			Type     string `json:"type"`
			PID      int    `json:"pid"`
			ExitCode int    `json:"exitCode"`
		}
		if json.Unmarshal(payload, &control) == nil && control.Type == "ready" && control.PID > 0 {
			p.PID = control.PID
			ready = true
			p.ready <- nil
			continue
		}
		if json.Unmarshal(payload, &control) == nil && control.Type == "exit" {
			if p.options.OnExit != nil {
				p.options.OnExit(control.ExitCode)
			}
			continue
		}
		if p.options.OnData != nil {
			p.options.OnData(payload)
		}
	}
}

func (p *PtySession) SendInput(data []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return websocket.Message.Send(p.conn, data)
}

func (p *PtySession) Resize(cols, rows int) error {
	data, err := json.Marshal(map[string]any{"type": "resize", "cols": cols, "rows": rows})
	if err != nil {
		return err
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return websocket.Message.Send(p.conn, data)
}

func (p *PtySession) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PtySession) Kill() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := websocket.Message.Send(p.conn, []byte(`{"type":"kill"}`)); err != nil {
		_ = p.conn.Close()
		return err
	}
	return p.conn.Close()
}

func (p *PtySession) Close() error {
	return p.conn.Close()
}

func (p *PTY) Connect(pid int, options ...PtyCreateOptions) (*PtySession, error) {
	p.mu.Lock()
	session := p.sessions[pid]
	p.mu.Unlock()
	if session != nil {
		select {
		case <-session.done:
		default:
			return session, nil
		}
	}
	active := PtyCreateOptions{}
	if len(options) > 0 {
		active = options[0]
	}
	return p.open(context.Background(), active, pid)
}

func (p *PTY) SendInput(pid int, data []byte) error {
	session, err := p.Connect(pid)
	if err != nil {
		return err
	}
	return session.SendInput(data)
}

func (p *PTY) Resize(pid, cols, rows int) error {
	session, err := p.Connect(pid)
	if err != nil {
		return err
	}
	return session.Resize(cols, rows)
}

func (p *PTY) Kill(pid int) error {
	session, err := p.Connect(pid)
	if err != nil {
		return err
	}
	err = session.Kill()
	p.mu.Lock()
	delete(p.sessions, pid)
	p.mu.Unlock()
	return err
}

func (p *PTY) Run(ctx context.Context, command string, options CommandRunOptions) (*CommandResult, error) {
	request := protocol.ExecRequest{Command: []string{"/bin/sh", "-c", command}, WorkingDir: options.CWD, Env: options.Env, Stdin: options.Stdin, Background: options.Background}
	if options.Timeout > 0 {
		request.TimeoutMs = options.Timeout.Milliseconds()
	}
	var response protocol.ExecResponse
	if err := p.client.sandboxJSON(ctx, p.sandbox, http.MethodPost, protocol.PathPTYSession, request, &response); err != nil {
		return nil, err
	}
	result := commandResult(response)
	return &result, nil
}

func (s *Sandbox) Kill(ctx context.Context) error {
	return s.client.doJSON(ctx, http.MethodDelete, "/v1/sandboxes/"+urlQueryEscape(s.SandboxID), nil, nil)
}

func (s *Sandbox) GetInfo(ctx context.Context) (*Sandbox, error) {
	accessToken := s.AccessToken
	var out Sandbox
	if err := s.client.doJSON(ctx, http.MethodGet, "/v1/sandboxes/"+urlQueryEscape(s.SandboxID), nil, &out); err != nil {
		return nil, err
	}
	if out.AccessToken == "" {
		out.AccessToken = accessToken
	}
	return s.client.attachSandbox(&out), nil
}

func (s *Sandbox) SetTimeout(ctx context.Context, timeout time.Duration) error {
	accessToken := s.AccessToken
	seconds := int64(timeout / time.Second)
	if timeout > 0 && seconds == 0 {
		seconds = 1
	}
	var out Sandbox
	if err := s.client.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(s.SandboxID)+"/timeout", map[string]int64{"idleTimeoutSeconds": seconds}, &out); err != nil {
		return err
	}
	if out.AccessToken == "" {
		out.AccessToken = accessToken
	}
	*s = *s.client.attachSandbox(&out)
	return nil
}

func (s *Sandbox) Pause(ctx context.Context) error {
	return s.client.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(s.SandboxID)+"/suspend", nil, nil)
}

func (s *Sandbox) BetaPause(ctx context.Context) error {
	return s.Pause(ctx)
}

func (s *Sandbox) Resume(ctx context.Context) error {
	accessToken := s.AccessToken
	var out Sandbox
	if err := s.client.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+urlQueryEscape(s.SandboxID)+"/resume", nil, &out); err != nil {
		return err
	}
	if out.AccessToken == "" {
		out.AccessToken = accessToken
	}
	*s = *s.client.attachSandbox(&out)
	return nil
}

func (s *Sandbox) GetHost(port int) string {
	base := s.client.GatewayURL
	parsed, err := url.Parse(base)
	if err != nil || parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost" {
		return strings.TrimRight(base, "/") + "/" + fmt.Sprintf("%d-%s", port, url.PathEscape(s.SandboxID))
	}
	parsed.Host = fmt.Sprintf("%d-%s.%s", port, s.SandboxID, parsed.Hostname())
	return strings.TrimRight(parsed.String(), "/")
}

type SandboxResource struct {
	client *Client
}

func (r *SandboxResource) Create(ctx context.Context, templateID string, options SandboxCreateOptions) (*Sandbox, error) {
	payload := map[string]any{}
	raw, err := json.Marshal(options)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if templateID != "" {
		payload["templateId"] = templateID
	}
	if options.Timeout > 0 {
		payload["idleTimeoutSeconds"] = int64((options.Timeout + time.Second - 1) / time.Second)
	}
	var out Sandbox
	if err := r.client.doJSON(ctx, http.MethodPost, "/v1/sandboxes", payload, &out); err != nil {
		return nil, err
	}
	return r.client.attachSandbox(&out), nil
}

func (r *SandboxResource) Connect(ctx context.Context, sandboxID string) (*Sandbox, error) {
	return r.client.Connect(ctx, sandboxID)
}

func (r *SandboxResource) List(ctx context.Context, state, templateID, cursor string, limit int) ([]*Sandbox, string, error) {
	page, err := r.client.ListSandboxes(ctx, state, templateID, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	result := make([]*Sandbox, 0, len(page.Sandboxes))
	for i := range page.Sandboxes {
		result = append(result, r.client.attachSandbox(&page.Sandboxes[i]))
	}
	return result, page.NextCursor, nil
}

func (c *Client) Sandboxes() *SandboxResource {
	return &SandboxResource{client: c}
}

type VolumeResource struct{ client *Client }

func (r *VolumeResource) Create(ctx context.Context, request any) (*Volume, error) {
	return r.client.CreateVolume(ctx, request)
}

func (r *VolumeResource) List(ctx context.Context) ([]Volume, error) {
	return r.client.ListVolumes(ctx)
}

type TemplateResource struct{ client *Client }

func (r *TemplateResource) List(ctx context.Context) ([]Template, error) {
	return r.client.ListTemplates(ctx)
}

func (r *TemplateResource) Get(ctx context.Context, templateID string) (*Template, error) {
	return r.client.GetTemplate(ctx, templateID)
}

func (r *TemplateResource) Exists(ctx context.Context, templateID string) (bool, error) {
	_, err := r.Get(ctx, templateID)
	if err == nil {
		return true, nil
	}
	if apiErr, ok := err.(*APIError); ok && apiErr.Status == http.StatusNotFound {
		return false, nil
	}
	return false, err
}

type E2B struct {
	Client   *Client
	Sandbox  *SandboxResource
	Volume   *VolumeResource
	Template *TemplateResource
}

func NewE2B(options ClientOptions) *E2B {
	client := NewClientWithOptions(options)
	return &E2B{Client: client, Sandbox: &SandboxResource{client: client}, Volume: &VolumeResource{client: client}, Template: &TemplateResource{client: client}}
}

func commandResult(response protocol.ExecResponse) CommandResult {
	return CommandResult{PID: response.PID, Stdout: response.Stdout, Stderr: response.Stderr, ExitCode: response.ExitCode, TimedOut: response.TimedOut, Duration: time.Duration(response.DurationMs) * time.Millisecond}
}

func (c *Client) sandboxJSON(ctx context.Context, sandbox *Sandbox, method, path string, input, output any) error {
	body, err := json.Marshal(input)
	if input == nil {
		body = nil
	}
	if err != nil {
		return err
	}
	_, err = c.sandboxRequest(ctx, sandbox, method, path, body, "application/json", output)
	return err
}

func (c *Client) sandboxBytes(ctx context.Context, sandbox *Sandbox, method, path string, body io.Reader, contentType string) ([]byte, error) {
	return c.sandboxRequest(ctx, sandbox, method, path, body, contentType, nil)
}

func (c *Client) sandboxRequest(ctx context.Context, sandbox *Sandbox, method, path string, body any, contentType string, output any) ([]byte, error) {
	var reader io.Reader
	switch value := body.(type) {
	case nil:
	case io.Reader:
		reader = value
	case []byte:
		reader = bytes.NewReader(value)
	default:
		return nil, fmt.Errorf("unsupported sandbox request body %T", body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.GatewayURL+path, reader)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("X-Kontinue-Sandbox-ID", sandbox.SandboxID)
	req.Header.Set("X-Kontinue-Token", sandbox.AccessToken)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var problem struct {
			Code    string `json:"code"`
			Message string `json:"error"`
		}
		_ = json.Unmarshal(raw, &problem)
		return nil, &APIError{Status: resp.StatusCode, Code: problem.Code, Message: problem.Message}
	}
	if output != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, output); err != nil {
			return nil, err
		}
	}
	return raw, nil
}
