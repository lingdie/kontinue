package arena

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func TestLazyReaderCloseRemovesStagingDir(t *testing.T) {
	root := t.TempDir()
	backing := filepath.Join(root, "staging", "sbx-1-exec-1", "backing.pages")
	reader, err := NewLazyPageReader(LazyReaderOptions{
		Manifest:    &protocol.SnapshotManifest{SnapshotID: "snap-1"},
		LocalCASDir: root,
		BackingFile: backing,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging", "sbx-1-exec-1")); !os.IsNotExist(err) {
		t.Fatalf("staging dir still present: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging")); err != nil {
		t.Fatalf("staging root removed: %v", err)
	}
}

func TestHealOrphansRemovesUntrackedViewsOnly(t *testing.T) {
	root := t.TempDir()
	views := filepath.Join(root, "views")
	cas := filepath.Join(root, "cas")
	live := filepath.Join(views, "sbx-live-exec-1")
	stale := filepath.Join(views, "sbx-stale-exec-2")
	if err := os.MkdirAll(live, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "checkpoint.img"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cas, "staging", "sbx-stale-exec-2"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cas, "staging", "sbx-live-exec-1"), 0o700); err != nil {
		t.Fatal(err)
	}
	m := &Manager{
		cfg:         ManagerConfig{ViewsDir: views, L2CASDir: cas},
		activeViews: map[string]ViewCloser{},
	}
	m.trackView(live, &plainDirHandle{dir: live})

	n, err := m.HealOrphans(context.Background(), "missing-runsc")
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("cleaned %d, want view and staging", n)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale view remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cas, "staging", "sbx-stale-exec-2")); !os.IsNotExist(err) {
		t.Fatalf("stale staging remains: %v", err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live view removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cas, "staging", "sbx-live-exec-1")); err != nil {
		t.Fatalf("live staging removed: %v", err)
	}
}
