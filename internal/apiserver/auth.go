package apiserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TenantKeys map[[32]byte]string

func LoadTenantKeys(path string) (TenantKeys, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries map[string]string
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, errors.New("invalid tenant credentials file")
	}
	if len(entries) == 0 {
		return nil, errors.New("tenant credentials file is empty")
	}
	keys := make(TenantKeys, len(entries))
	for tenant, token := range entries {
		if strings.TrimSpace(tenant) == "" || strings.TrimSpace(token) == "" {
			return nil, errors.New("empty tenant or credential")
		}
		hash := sha256.Sum256([]byte(token))
		if _, ok := keys[hash]; ok {
			return nil, errors.New("credential maps to multiple tenants")
		}
		keys[hash] = tenant
	}
	return keys, nil
}

type principal struct{ tenant, sandboxID, token string }
type principalKey struct{}

const localTenant = "local"

func tenantID(ctx context.Context) string {
	p, _ := ctx.Value(principalKey{}).(principal)
	if p.tenant == "" {
		return localTenant
	}
	return p.tenant
}
func visible(ctx context.Context, owner string) bool {
	p, ok := ctx.Value(principalKey{}).(principal)
	if !ok {
		return true
	} // internal worker
	if owner == "" {
		owner = localTenant
	} // records created before tenant ownership
	return owner == p.tenant
}

var errCredential = errors.New("invalid sandbox credential")
var errOtherTenant = errors.New("resource not found")

func (s *Server) sandbox(ctx context.Context, id string) (*store.SandboxRecord, error) {
	b, err := s.cfg.Meta.GetSandbox(ctx, id)
	p, _ := ctx.Value(principalKey{}).(principal)
	if p.sandboxID != "" {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errCredential
		}
		if err != nil {
			return nil, err
		}
		if p.sandboxID != id || p.token == "" || subtle.ConstantTimeCompare([]byte(b.AccessToken), []byte(p.token)) != 1 {
			return nil, errCredential
		}
		return b, nil
	}
	if err == nil && !visible(ctx, b.TenantID) {
		return nil, errOtherTenant
	}
	return b, err
}

func (s *Server) template(ctx context.Context, id string) (*store.TemplateRecord, error) {
	t, err := s.cfg.Meta.GetTemplate(ctx, id)
	if err == nil && !visible(ctx, t.TenantID) {
		return nil, errOtherTenant
	}
	return t, err
}

func (s *Server) tenantHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := localTenant
		if s.cfg.TenantKeys != nil {
			header := r.Header.Get("Authorization")
			scheme, token, ok := strings.Cut(header, " ")
			var exists bool
			tenant, exists = s.cfg.TenantKeys[sha256.Sum256([]byte(token))]
			if !ok || !strings.EqualFold(scheme, "Bearer") || !exists {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeErr(w, http.StatusUnauthorized, "tenant credential required")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, principal{tenant: tenant})))
	})
}

// The gateway can wake or verify only the sandbox whose access credential it presents.
func (s *Server) gatewayAction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p := principal{sandboxID: id, token: r.Header.Get(protocol.HeaderKontinueToken)}
	if p.token == "" {
		writeErr(w, http.StatusUnauthorized, errCredential.Error())
		return
	}
	ctx := context.WithValue(r.Context(), principalKey{}, p)
	var err error
	switch r.PathValue("action") {
	case "authorize":
		_, err = s.sandbox(ctx, id)
	case "resume":
		_, err = s.ResumeSandbox(ctx, id)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeLifecycleError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeLifecycleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errOtherTenant), errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "resource not found")
	case errors.Is(err, errCredential):
		writeErr(w, http.StatusUnauthorized, errCredential.Error())
	case errors.Is(err, store.ErrSandboxExists), errors.Is(err, store.ErrTransitionLocked), errors.Is(err, store.ErrStaleExecutionFence):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrNoIdleCarrierSlot), status.Code(err) == codes.Unavailable:
		writeErr(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}

func createKeyDigest(ctx context.Context, key string) string {
	pair, _ := json.Marshal([2]string{tenantID(ctx), key})
	return fmt.Sprintf("%x", sha256.Sum256(pair))
}
