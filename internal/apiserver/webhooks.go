package apiserver

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
)

type CreateWebhookRequest struct {
	URL    string   `json:"url"`
	Events []string `json:"events,omitempty"`
	Secret string   `json:"secret,omitempty"`
}

type webhookResponse struct {
	WebhookID string    `json:"webhookId"`
	URL       string    `json:"url"`
	Events    []string  `json:"events,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	Secret    string    `json:"secret,omitempty"`
}

func publicWebhook(record *store.WebhookRecord, includeSecret bool) webhookResponse {
	response := webhookResponse{WebhookID: record.WebhookID, URL: record.URL, Events: append([]string(nil), record.Events...), CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt}
	if includeSecret {
		response.Secret = record.Secret
	}
	return response
}

func (s *Server) webhook(ctx context.Context, webhookID string) (*store.WebhookRecord, error) {
	record, err := s.cfg.Meta.GetWebhook(ctx, webhookID)
	if err == nil && !visible(ctx, record.TenantID) {
		return nil, errOtherTenant
	}
	return record, err
}

func (s *Server) handleWebhooks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		all, err := s.cfg.Meta.ListWebhooks(r.Context())
		if err != nil {
			writeLifecycleError(w, r, err)
			return
		}
		out := make([]webhookResponse, 0, len(all))
		for i := range all {
			if visible(r.Context(), all[i].TenantID) {
				out = append(out, publicWebhook(&all[i], false))
			}
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var req CreateWebhookRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			writeProblem(w, r, invalidJSON, err)
			return
		}
		parsed, err := url.Parse(req.URL)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
			writeProblem(w, r, invalidJSON, fmt.Errorf("webhook URL must be an absolute HTTP or HTTPS URL"))
			return
		}
		events := normalizeWebhookEvents(req.Events)
		if len(events) == 0 {
			writeProblem(w, r, invalidJSON, fmt.Errorf("webhook events cannot be empty"))
			return
		}
		if req.Secret == "" {
			req.Secret = "whsec_" + randHex(24)
		}
		record := &store.WebhookRecord{TenantID: tenantID(r.Context()), WebhookID: "wh-" + randHex(10), URL: req.URL, Events: events, Secret: req.Secret}
		if err := s.cfg.Meta.CreateWebhook(r.Context(), record); err != nil {
			writeLifecycleError(w, r, err)
			return
		}
		writeJSON(w, http.StatusCreated, publicWebhook(record, true))
	default:
		writeProblem(w, r, methodNotAllowed, nil)
	}
}

func normalizeWebhookEvents(events []string) []string {
	seen := make(map[string]struct{}, len(events))
	out := make([]string, 0, len(events))
	for _, event := range events {
		event = strings.TrimSpace(event)
		if event == "" {
			continue
		}
		if _, ok := seen[event]; ok {
			continue
		}
		seen[event] = struct{}{}
		out = append(out, event)
		if len(out) == 32 {
			break
		}
	}
	if len(out) == 0 {
		return []string{"*"}
	}
	return out
}

func (s *Server) handleWebhookByID(w http.ResponseWriter, r *http.Request) {
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/webhooks/"), "/")
	if id == "" {
		writeProblem(w, r, resourceNotFound, nil)
		return
	}
	record, err := s.webhook(r.Context(), id)
	if err != nil {
		writeLifecycleError(w, r, err)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, publicWebhook(record, false))
	case http.MethodDelete:
		if err := s.cfg.Meta.DeleteWebhook(r.Context(), id); err != nil {
			writeLifecycleError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeProblem(w, r, methodNotAllowed, nil)
	}
}

type webhookEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"createdAt"`
	Data      any       `json:"data"`
}

func webhookMatches(events []string, eventType string) bool {
	for _, event := range events {
		if event == "*" || event == eventType {
			return true
		}
		if strings.HasSuffix(event, ".*") && strings.HasPrefix(eventType, strings.TrimSuffix(event, "*")) {
			return true
		}
	}
	return false
}

func (s *Server) emitWebhookEvent(ctx context.Context, tenant, eventType string, data any) {
	records, err := s.cfg.Meta.ListWebhooks(context.WithoutCancel(ctx))
	if err != nil {
		return
	}
	payload, err := json.Marshal(webhookEvent{ID: "evt-" + randHex(12), Type: eventType, CreatedAt: time.Now().UTC(), Data: data})
	if err != nil {
		return
	}
	for _, record := range records {
		if record.TenantID != tenant || !webhookMatches(record.Events, eventType) {
			continue
		}
		record := record
		go s.deliverWebhook(record, payload)
	}
}

func (s *Server) deliverWebhook(record store.WebhookRecord, payload []byte) {
	mac := hmac.New(sha256.New, []byte(record.Secret))
	_, _ = mac.Write(payload)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, record.URL, bytes.NewReader(payload))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Kontinue-Event", "lifecycle")
			req.Header.Set("X-Kontinue-Signature", signature)
			resp, requestErr := s.webhookClient.Do(req)
			if requestErr == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
				_ = resp.Body.Close()
				cancel()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return
				}
			} else {
				cancel()
			}
		} else {
			cancel()
		}
		time.Sleep(time.Duration(1<<attempt) * 100 * time.Millisecond)
	}
}
