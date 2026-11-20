package apiserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// KubeletCheckpointer triggers Kubernetes's native node-local Checkpoint API:
// POST https://<node-ip>:<port>/checkpoint/{namespace}/{pod}/{container}?timeout=60
type KubeletCheckpointer interface {
	CheckpointContainer(ctx context.Context, nodeIP string, kubeletPort int, namespace, podName, containerName string, timeoutSec int) ([]string, error)
}

type HTTPSKubeletClient struct {
	httpClient  *http.Client
	bearerToken string
	tokenPath   string
}

func NewHTTPSKubeletClient(bearerToken string, insecureSkipVerify bool) *HTTPSKubeletClient {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify,
	}
	baseTr := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConnsPerHost: 16,
	}
	return &HTTPSKubeletClient{
		httpClient: &http.Client{
			Timeout:   90 * time.Second,
			Transport: telemetry.NewTracingTransport("kubelet-checkpoint-client", baseTr),
		},
		bearerToken: bearerToken,
		tokenPath:   "/var/run/secrets/kubernetes.io/serviceaccount/token",
	}
}

type kubeletCheckpointResp struct {
	Items []string `json:"items"`
}

func (c *HTTPSKubeletClient) CheckpointContainer(
	ctx context.Context,
	nodeIP string,
	kubeletPort int,
	namespace, podName, containerName string,
	timeoutSec int,
) ([]string, error) {
	if kubeletPort <= 0 {
		kubeletPort = 10250
	}
	if timeoutSec <= 0 {
		timeoutSec = 60
	}
	ctx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "kubelet.checkpoint_api",
		attribute.String("k8s.namespace", namespace),
		attribute.String("k8s.pod", podName),
		attribute.String("k8s.container", containerName),
		attribute.String("k8s.node_ip", nodeIP),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	urlStr := fmt.Sprintf("https://%s:%d/checkpoint/%s/%s/%s?timeout=%d",
		nodeIP, kubeletPort, namespace, podName, containerName, timeoutSec)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, nil)
	if err != nil {
		retErr = err
		return nil, err
	}
	tok := c.resolveToken()
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		retErr = fmt.Errorf("POST kubelet checkpoint %s/%s: %w", namespace, podName, err)
		return nil, retErr
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		retErr = fmt.Errorf("kubelet checkpoint status %d: %s", resp.StatusCode, string(body))
		return nil, retErr
	}
	var parsed kubeletCheckpointResp
	_ = json.Unmarshal(body, &parsed)
	return parsed.Items, nil
}

func (c *HTTPSKubeletClient) resolveToken() string {
	if c.bearerToken != "" {
		return c.bearerToken
	}
	if b, err := os.ReadFile(c.tokenPath); err == nil {
		return string(b)
	}
	return ""
}
