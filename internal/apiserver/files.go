package apiserver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type SignedFileURLRequest struct {
	Path             string `json:"path"`
	Operation        string `json:"operation"`
	ExpiresInSeconds int64  `json:"expiresInSeconds,omitempty"`
}

type SignedFileURLResponse struct {
	URL       string `json:"url"`
	Path      string `json:"path"`
	Operation string `json:"operation"`
	ExpiresAt int64  `json:"expiresAtUnixSeconds"`
}

func normalizeFileOperation(operation string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(operation)) {
	case "read", "download":
		return "read", true
	case "write", "upload":
		return "write", true
	default:
		return "", false
	}
}

func (s *Server) createSignedFileURL(ctx context.Context, sandboxID string, req SignedFileURLRequest) (*SignedFileURLResponse, error) {
	if _, err := s.sandbox(ctx, sandboxID); err != nil {
		return nil, err
	}
	if req.Path == "" || !filepath.IsAbs(req.Path) {
		return nil, fmt.Errorf("file URL path must be absolute")
	}
	operation, ok := normalizeFileOperation(req.Operation)
	if !ok {
		return nil, fmt.Errorf("file URL operation must be read or write")
	}
	expiresIn := req.ExpiresInSeconds
	if expiresIn == 0 {
		expiresIn = 300
	}
	if expiresIn < 1 || expiresIn > 3600 {
		return nil, fmt.Errorf("file URL expiry must be between 1 and 3600 seconds")
	}
	expiresAt := time.Now().Unix() + expiresIn
	signature := s.signFileURL(sandboxID, operation, req.Path, expiresAt)
	query := url.Values{}
	query.Set("path", req.Path)
	query.Set("operation", operation)
	query.Set("expiresAt", strconv.FormatInt(expiresAt, 10))
	query.Set("signature", signature)
	return &SignedFileURLResponse{
		URL:       "/v1/sandboxes/" + url.PathEscape(sandboxID) + "/files?" + query.Encode(),
		Path:      req.Path,
		Operation: operation,
		ExpiresAt: expiresAt,
	}, nil
}

func (s *Server) signFileURL(sandboxID, operation, path string, expiresAt int64) string {
	mac := hmac.New(sha256.New, s.fileURLKey)
	_, _ = io.WriteString(mac, sandboxID)
	_, _ = io.WriteString(mac, "\x00")
	_, _ = io.WriteString(mac, operation)
	_, _ = io.WriteString(mac, "\x00")
	_, _ = io.WriteString(mac, path)
	_, _ = io.WriteString(mac, "\x00")
	_, _ = io.WriteString(mac, strconv.FormatInt(expiresAt, 10))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Server) verifyFileURL(r *http.Request, sandboxID string) (string, string, error) {
	query := r.URL.Query()
	path := query.Get("path")
	operation, ok := normalizeFileOperation(query.Get("operation"))
	if !ok || path == "" || !filepath.IsAbs(path) {
		return "", "", errCredential
	}
	expiresAt, err := strconv.ParseInt(query.Get("expiresAt"), 10, 64)
	if err != nil || expiresAt < time.Now().Unix() {
		return "", "", errCredential
	}
	provided, err := base64.RawURLEncoding.DecodeString(query.Get("signature"))
	if err != nil {
		return "", "", errCredential
	}
	expected, err := base64.RawURLEncoding.DecodeString(s.signFileURL(sandboxID, operation, path, expiresAt))
	if err != nil || !hmac.Equal(provided, expected) {
		return "", "", errCredential
	}
	return operation, path, nil
}

func (s *Server) serveSignedFile(w http.ResponseWriter, r *http.Request, sandboxID string) {
	operation, path, err := s.verifyFileURL(r, sandboxID)
	if err != nil {
		writeProblem(w, r, invalidSandboxCredential, err)
		return
	}
	sbx, err := s.sandbox(r.Context(), sandboxID)
	if err != nil {
		writeLifecycleError(w, r, err)
		return
	}
	if sbx.State == store.SandboxStateSuspended {
		sbx, err = s.ResumeSandbox(r.Context(), sandboxID)
		if err != nil {
			writeLifecycleError(w, r, err)
			return
		}
	}
	if sbx.State != store.SandboxStateRunning {
		writeProblem(w, r, resourceNotFound, fmt.Errorf("sandbox is not running"))
		return
	}
	route, err := s.routeForExecution(r.Context(), sbx)
	if err != nil {
		writeLifecycleError(w, r, err)
		return
	}
	addr := route.PodIP
	if _, _, splitErr := net.SplitHostPort(addr); splitErr != nil {
		addr = net.JoinHostPort(addr, strconv.Itoa(protocol.DefaultKtdPort))
	}

	switch {
	case r.Method == http.MethodGet && operation == "read":
		body, length, err := s.ktdClient.OpenFile(r.Context(), addr, sbx.AccessToken, path)
		if err != nil {
			writeLifecycleError(w, r, err)
			return
		}
		defer body.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		if length >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		}
		_, _ = io.Copy(w, body)
	case r.Method == http.MethodPut && operation == "write":
		if err := s.ktdClient.WriteFileStream(r.Context(), addr, sbx.AccessToken, path, r.Body); err != nil {
			writeLifecycleError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeProblem(w, r, methodNotAllowed, nil)
	}
}
