package apiserver

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
)

func TestWebhookDeliverySignsMatchingLifecycleEvent(t *testing.T) {
	type delivery struct {
		header  string
		payload []byte
	}
	deliveries := make(chan delivery, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		deliveries <- delivery{header: r.Header.Get("X-Kontinue-Signature"), payload: payload}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	meta := store.NewMemoryMetadataStore()
	secret := "webhook-test-secret"
	if err := meta.CreateWebhook(context.Background(), &store.WebhookRecord{
		TenantID:  localTenant,
		WebhookID: "wh-test",
		URL:       receiver.URL,
		Events:    []string{"sandbox.created"},
		Secret:    secret,
	}); err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{Meta: meta, Catalog: store.NewMemoryRuntimeCatalog()})
	s.emitWebhookEvent(context.Background(), localTenant, "sandbox.created", map[string]string{"sandboxId": "sbx-test"})

	select {
	case received := <-deliveries:
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(received.payload)
		expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if received.header != expected {
			t.Fatalf("signature = %q, want %q", received.header, expected)
		}
		var event webhookEvent
		if err := json.Unmarshal(received.payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.Type != "sandbox.created" {
			t.Fatalf("event type = %q", event.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webhook was not delivered")
	}
}

func TestWebhookHTTPAPIOnlyReturnsSecretOnCreate(t *testing.T) {
	s := NewServer(ServerConfig{Meta: store.NewMemoryMetadataStore(), Catalog: store.NewMemoryRuntimeCatalog()})
	server := httptest.NewServer(s.Handler())
	defer server.Close()

	body := bytes.NewBufferString(`{"url":"https://example.test/events","events":["sandbox.*"],"secret":"fixed-secret"}`)
	response, err := http.Post(server.URL+"/v1/webhooks", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", response.StatusCode)
	}
	var created webhookResponse
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.Secret != "fixed-secret" {
		t.Fatalf("create response did not return configured secret")
	}

	response, err = http.Get(server.URL + "/v1/webhooks/" + created.WebhookID)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var fetched webhookResponse
	if err := json.NewDecoder(response.Body).Decode(&fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.Secret != "" {
		t.Fatalf("get response leaked secret %q", fetched.Secret)
	}
}
