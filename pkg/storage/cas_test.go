package storage_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
)

func TestCASLocalAndHTTPRangeBackend(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocalFSBackend(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFSBackend: %v", err)
	}

	raw := []byte("0123456789abcdef-kontinue-pages")
	key := storage.ChunkKey("deadbeef1234")
	if err := local.PutBlob(ctx, key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	sub, err := local.GetBlobRange(ctx, key, 10, 6)
	if err != nil || string(sub) != "abcdef" {
		t.Fatalf("GetBlobRange got (%q, %v)", string(sub), err)
	}

	objs := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := strings.TrimPrefix(r.URL.Path, "/")
		switch r.Method {
		case http.MethodPut:
			var b bytes.Buffer
			_, _ = b.ReadFrom(r.Body)
			objs[k] = b.Bytes()
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			data, ok := objs[k]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
				parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
				s, _ := strconv.Atoi(parts[0])
				e, _ := strconv.Atoi(parts[1])
				if e >= len(data) {
					e = len(data) - 1
				}
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[s : e+1])
				return
			}
			_, _ = w.Write(data)
		}
	}))
	defer srv.Close()

	remote := storage.NewHTTPObjectStoreBackend(srv.URL, "tok", srv.Client())
	if err := remote.PutBlob(ctx, key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("HTTP PutBlob: %v", err)
	}
	rSub, err := remote.GetBlobRange(ctx, key, 17, 8)
	if err != nil || string(rSub) != "kontinue" {
		t.Fatalf("HTTP GetBlobRange got (%q, %v)", string(rSub), err)
	}

	m := &protocol.SnapshotManifest{SchemaVersion: 1, SnapshotID: "snap-m0", Kind: protocol.SnapshotKindGoldenTemplate}
	if _, err := local.PutManifest(ctx, "snap-m0", m); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
}
