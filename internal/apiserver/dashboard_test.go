package apiserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/store"
)

func TestDashboardSummaryScopesTenantAndOmitsSandboxCredentials(t *testing.T) {
	meta := store.NewMemoryMetadataStore()
	if err := meta.CreateSandbox(context.Background(), &store.SandboxRecord{
		SandboxID:   "sbx-dashboard",
		TenantID:    localTenant,
		AccessToken: "secret",
		State:       store.SandboxStateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if err := meta.CreateSandbox(context.Background(), &store.SandboxRecord{
		SandboxID: "sbx-other", TenantID: "other", State: store.SandboxStateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{Meta: meta, Catalog: store.NewMemoryRuntimeCatalog()})
	request := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d", response.Code)
	}
	var body struct {
		SandboxCounts map[string]int `json:"sandboxCounts"`
		Sandboxes     []struct {
			SandboxID   string `json:"sandboxId"`
			AccessToken string `json:"accessToken"`
		} `json:"sandboxes"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.SandboxCounts[string(store.SandboxStateRunning)] != 1 || len(body.Sandboxes) != 1 {
		t.Fatalf("unexpected dashboard scope: %+v", body)
	}
	if body.Sandboxes[0].AccessToken != "" {
		t.Fatal("dashboard leaked sandbox access token")
	}
}
