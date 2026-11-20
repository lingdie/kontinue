package apiserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
)

type unreliableCAS struct {
	*storage.LocalFSBackend
	unreadable  string
	undeletable string
	deletes     int
}

func (c *unreliableCAS) GetManifest(ctx context.Context, id string) (*protocol.SnapshotManifest, error) {
	if id == c.unreadable {
		return nil, errors.New("temporary manifest failure")
	}
	return c.LocalFSBackend.GetManifest(ctx, id)
}
func (c *unreliableCAS) DeleteBlob(ctx context.Context, key string) error {
	c.deletes++
	if key == c.undeletable {
		return errors.New("temporary delete failure")
	}
	return c.LocalFSBackend.DeleteBlob(ctx, key)
}

func TestSnapshotGCRetriesWithoutDeletingSharedPages(t *testing.T) {
	ctx := context.Background()
	backend, err := storage.NewLocalFSBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cas := &unreliableCAS{LocalFSBackend: backend}
	meta := store.NewMemoryMetadataStore()
	server := NewServer(ServerConfig{Meta: meta, Catalog: store.NewMemoryRuntimeCatalog(), CAS: cas})
	save := func(id string, doomed bool, hashes ...string) {
		t.Helper()
		mf := &protocol.SnapshotManifest{SnapshotID: id, PagesImgTotalSize: int64(len(hashes))}
		for i, h := range hashes {
			mf.Chunks = append(mf.Chunks, protocol.ChunkRef{Offset: int64(i), Length: 1, Blake3Hash: h})
			if err := cas.PutBlob(ctx, storage.ChunkKey(h), strings.NewReader("x"), 1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := cas.PutManifest(ctx, id, mf); err != nil {
			t.Fatal(err)
		}
		if err := meta.CreateSnapshot(ctx, &store.SnapshotRecord{SnapshotID: id, Superseded: doomed, TotalMemoryBytes: mf.PagesImgTotalSize}); err != nil {
			t.Fatal(err)
		}
	}
	save("old", true, "shared", "unreferenced", "publishing")
	save("active", false, "shared")
	cas.unreadable = "active"
	if err := server.collectSnapshots(ctx); err == nil || cas.deletes != 0 {
		t.Fatalf("incomplete mark: err=%v deletes=%d", err, cas.deletes)
	}
	cas.unreadable = ""
	publish, err := meta.LockSnapshotSet(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	err = server.collectSnapshots(deadline)
	cancel()
	save("new", false, "publishing")
	publish()
	if !errors.Is(err, context.DeadlineExceeded) || cas.deletes != 0 {
		t.Fatalf("GC overlapped pending upload: %v", err)
	}
	cas.undeletable = storage.ArtifactKey("old", protocol.ManifestFileName)
	if err := server.collectSnapshots(ctx); err == nil {
		t.Fatal("expected delete error")
	}
	if _, err := meta.GetSnapshot(ctx, "old"); err != nil {
		t.Fatal("lost retry record", err)
	}
	cas.undeletable = ""
	if err := server.collectSnapshots(ctx); err != nil {
		t.Fatal(err)
	}
	for hash, want := range map[string]bool{"shared": true, "unreferenced": false, "publishing": true} {
		got, err := cas.HasBlob(ctx, storage.ChunkKey(hash))
		if err != nil || got != want {
			t.Fatalf("chunk %s: exists=%t err=%v", hash, got, err)
		}
	}
	if _, err := meta.GetSnapshot(ctx, "old"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("candidate remains: %v", err)
	}
}
