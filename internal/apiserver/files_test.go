package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
)

func TestSignedFileURLBindsSandboxOperationPathAndExpiry(t *testing.T) {
	meta := store.NewMemoryMetadataStore()
	if err := meta.CreateSandbox(context.Background(), &store.SandboxRecord{
		SandboxID:   "box",
		TenantID:    localTenant,
		AccessToken: "sandbox-token",
		State:       store.SandboxStateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{Meta: meta, Catalog: store.NewMemoryRuntimeCatalog(), FileURLSecret: "test-secret"})
	ctx := context.WithValue(context.Background(), principalKey{}, principal{tenant: localTenant})
	issued, err := s.createSignedFileURL(ctx, "box", SignedFileURLRequest{Path: "/tmp/data.bin", Operation: "download", ExpiresInSeconds: 60})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, issued.URL, nil)
	operation, path, err := s.verifyFileURL(request, "box")
	if err != nil || operation != "read" || path != "/tmp/data.bin" {
		t.Fatalf("verify signed URL = (%q, %q, %v)", operation, path, err)
	}

	request.URL.RawQuery = url.Values{
		"path":      []string{"/tmp/other.bin"},
		"operation": []string{"read"},
		"expiresAt": []string{request.URL.Query().Get("expiresAt")},
		"signature": []string{request.URL.Query().Get("signature")},
	}.Encode()
	if _, _, err := s.verifyFileURL(request, "box"); err == nil {
		t.Fatal("expected path mutation to invalidate signature")
	}

	request.URL.RawQuery = url.Values{
		"path":      []string{"/tmp/data.bin"},
		"operation": []string{"read"},
		"expiresAt": []string{strconv.FormatInt(time.Now().Unix()-1, 10)},
		"signature": []string{"invalid"},
	}.Encode()
	if _, _, err := s.verifyFileURL(request, "box"); err == nil {
		t.Fatal("expected expired URL to be rejected")
	}
}
