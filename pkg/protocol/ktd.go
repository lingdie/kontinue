package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// DefaultHTTPTransportWrapper is wired by pkg/telemetry at init time so all outbound HTTP clients
// automatically inject W3C traceparent headers and emit client spans.
var DefaultHTTPTransportWrapper func(component string, base http.RoundTripper) http.RoundTripper

type InitPayload struct {
	SandboxID      string            `json:"sandboxId"`
	ExecutionID    int64             `json:"executionId"`
	LifecycleID    string            `json:"lifecycleId"`
	Hostname       string            `json:"hostname"`
	MachineID      string            `json:"machineId,omitempty"`
	AccessToken    string            `json:"accessToken"`
	EnvVars        map[string]string `json:"envVars,omitempty"`
	EntropySeed    []byte            `json:"entropySeed,omitempty"`
	WallClockUnixN int64             `json:"wallClockUnixNano"`
	SkipThaw       bool              `json:"skipThaw,omitempty"`
}

type InitResponse struct {
	SandboxID        string `json:"sandboxId"`
	ExecutionID      int64  `json:"executionId"`
	ClockDeltaMillis int64  `json:"clockDeltaMillis"`
	Thawed           bool   `json:"thawed"`
	MachineIDUpdated bool   `json:"machineIdUpdated"`
	HostnameUpdated  bool   `json:"hostnameUpdated"`
}

type FreezeRequest struct {
	Reason              string `json:"reason"`
	DrainExternalTCP    bool   `json:"drainExternalTcp"`
	SyncFilesystems     bool   `json:"syncFilesystems"`
	WaitInFlightTimeout int32  `json:"waitInFlightTimeoutMs,omitempty"`
}

type FreezeResponse struct {
	Frozen               bool  `json:"frozen"`
	Synced               bool  `json:"synced"`
	ClosedExternalConns  int   `json:"closedExternalConns"`
	ActiveManagedProcs   int   `json:"activeManagedProcs"`
	QuiesceDurationMicro int64 `json:"quiesceDurationMicros"`
}

type HealthResponse struct {
	LifecycleID   string `json:"lifecycleId,omitempty"`
	Healthy       bool   `json:"healthy"`
	Version       string `json:"version"`
	SandboxID     string `json:"sandboxId"`
	ExecutionID   int64  `json:"executionId"`
	Frozen        bool   `json:"frozen"`
	Initialized   bool   `json:"initialized"`
	UptimeMillis  int64  `json:"uptimeMillis"`
	ClockSkewNano int64  `json:"clockSkewNano"`
}

type ExecRequest struct {
	Command    []string          `json:"command"`
	WorkingDir string            `json:"workingDir,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Stdin      string            `json:"stdin,omitempty"`
	TimeoutMs  int64             `json:"timeoutMs,omitempty"`
	Background bool              `json:"background,omitempty"`
}

type ExecResponse struct {
	PID        int    `json:"pid"`
	ExitCode   int    `json:"exitCode"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	DurationMs int64  `json:"durationMs"`
	TimedOut   bool   `json:"timedOut,omitempty"`
}

type ProcessInfo struct {
	PID       int      `json:"pid"`
	Command   []string `json:"command"`
	StartedAt int64    `json:"startedAtUnixMs"`
	Running   bool     `json:"running"`
	ExitCode  int      `json:"exitCode,omitempty"`
}

type ProcessLogsResponse struct {
	PID             int    `json:"pid"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	OutputTruncated bool   `json:"outputTruncated"`
	Running         bool   `json:"running"`
	ExitCode        int    `json:"exitCode"`
}

type FileWriteRequest struct {
	Path    string `json:"path"`
	Content []byte `json:"content"`
	Mode    uint32 `json:"mode,omitempty"`
}

type FileRenameRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type FileInfo struct {
	Path             string `json:"path"`
	IsDir            bool   `json:"isDir"`
	IsFile           bool   `json:"isFile"`
	Size             int64  `json:"size"`
	ModifiedAtUnixMs *int64 `json:"modifiedAtUnixMs,omitempty"`
}

type FileEntry struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	IsDir  bool   `json:"isDir"`
	IsFile bool   `json:"isFile"`
	Size   int64  `json:"size"`
}

// KtdClient talks to the Rust in-sandbox guest daemon (ktd :49983).
type KtdClient struct {
	httpClient *http.Client
}

func NewKtdClient(timeout time.Duration) *KtdClient {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	var tr http.RoundTripper = &http.Transport{
		ExpectContinueTimeout: 5 * time.Second,
		MaxIdleConnsPerHost:   32,
	}
	if DefaultHTTPTransportWrapper != nil {
		tr = DefaultHTTPTransportWrapper("ktd-client", tr)
	}
	return &KtdClient{
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: tr,
		},
	}
}

func ktdURL(addr, path string) string {
	return fmt.Sprintf("http://%s%s", addr, path)
}

func (c *KtdClient) Health(ctx context.Context, addr, token string) (*HealthResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ktdURL(addr, PathInternalHealth), nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set(HeaderKontinueToken, token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ktd health status %d", resp.StatusCode)
	}
	var out HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *KtdClient) Freeze(ctx context.Context, addr, bootstrapToken string, fReq FreezeRequest) (*FreezeResponse, error) {
	body, err := json.Marshal(fReq)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ktdURL(addr, PathInternalFreeze), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderBootstrapToken, bootstrapToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("ktd freeze failed (%d): %s", resp.StatusCode, string(msg))
	}
	var out FreezeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *KtdClient) Thaw(ctx context.Context, addr, bootstrapToken string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ktdURL(addr, PathInternalThaw), nil)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderBootstrapToken, bootstrapToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("ktd thaw failed (%d)", resp.StatusCode)
	}
	return nil
}

func (c *KtdClient) HeldInit(ctx context.Context, addr, bootstrapToken string, payload InitPayload) (*InitResponse, error) {
	if payload.WallClockUnixN == 0 {
		payload.WallClockUnixN = time.Now().UnixNano()
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ktdURL(addr, PathInternalInit), bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set(HeaderBootstrapToken, bootstrapToken)
	if payload.SandboxID != "" {
		req.Header.Set(HeaderKontinueSandboxID, payload.SandboxID)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("ktd init failed (%d): %s", resp.StatusCode, string(msg))
	}
	if resp.StatusCode == http.StatusNoContent {
		return &InitResponse{SandboxID: payload.SandboxID, ExecutionID: payload.ExecutionID, Thawed: !payload.SkipThaw}, nil
	}
	var out InitResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *KtdClient) Exec(ctx context.Context, addr, accessToken string, execReq ExecRequest) (*ExecResponse, error) {
	raw, err := json.Marshal(execReq)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ktdURL(addr, PathProcessExec), bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("ktd exec failed (%d): %s", resp.StatusCode, string(msg))
	}
	var out ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *KtdClient) KillProcess(ctx context.Context, addr, accessToken string, pid int) error {
	u := ktdURL(addr, PathProcessKill) + "?pid=" + url.QueryEscape(fmt.Sprint(pid))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("ktd kill process failed (%d): %s", resp.StatusCode, string(msg))
	}
	return nil
}

func (c *KtdClient) ProcessLogs(ctx context.Context, addr, accessToken string, pid int) (*ProcessLogsResponse, error) {
	u := ktdURL(addr, PathProcessLogs) + "?pid=" + url.QueryEscape(fmt.Sprint(pid))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("ktd process logs failed (%d): %s", resp.StatusCode, string(msg))
	}
	var out ProcessLogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *KtdClient) WriteFile(ctx context.Context, addr, accessToken string, wReq FileWriteRequest) error {
	raw, err := json.Marshal(wReq)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, ktdURL(addr, PathFiles), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ktd write file failed (%d)", resp.StatusCode)
	}
	return nil
}

func (c *KtdClient) WriteFileStream(ctx context.Context, addr, accessToken, path string, content io.Reader) error {
	u := ktdURL(addr, PathFilesStream) + "?path=" + url.QueryEscape(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, content)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ktd streamed write failed (%d)", resp.StatusCode)
	}
	return nil
}

func (c *KtdClient) ReadFile(ctx context.Context, addr, accessToken, path string) ([]byte, error) {
	body, _, err := c.OpenFile(ctx, addr, accessToken, path)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

func (c *KtdClient) OpenFile(ctx context.Context, addr, accessToken, path string) (io.ReadCloser, int64, error) {
	u := ktdURL(addr, PathFiles) + "?path=" + url.QueryEscape(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, 0, fmt.Errorf("ktd read file failed (%d)", resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, nil
}

func (c *KtdClient) StatFile(ctx context.Context, addr, accessToken, path string) (*FileInfo, error) {
	u := ktdURL(addr, PathFilesInfo) + "?path=" + url.QueryEscape(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ktd stat file failed (%d)", resp.StatusCode)
	}
	var out FileInfo
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *KtdClient) ListFiles(ctx context.Context, addr, accessToken, path string) ([]FileEntry, error) {
	u := ktdURL(addr, PathFilesList) + "?path=" + url.QueryEscape(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ktd list files failed (%d)", resp.StatusCode)
	}
	var out []FileEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *KtdClient) MakeDir(ctx context.Context, addr, accessToken, path string) error {
	return c.fileMutation(ctx, addr, http.MethodPost, PathFilesMkdir, accessToken, path)
}

func (c *KtdClient) RemoveFile(ctx context.Context, addr, accessToken, path string) error {
	return c.fileMutation(ctx, addr, http.MethodDelete, PathFiles, accessToken, path)
}

func (c *KtdClient) RenameFile(ctx context.Context, addr, accessToken string, renameReq FileRenameRequest) error {
	raw, err := json.Marshal(renameReq)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ktdURL(addr, PathFilesRename), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ktd rename file failed (%d)", resp.StatusCode)
	}
	return nil
}

func (c *KtdClient) fileMutation(ctx context.Context, addr, method, path, accessToken, filePath string) error {
	u := ktdURL(addr, path) + "?path=" + url.QueryEscape(filePath)
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderKontinueToken, accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ktd file mutation failed (%d)", resp.StatusCode)
	}
	return nil
}
