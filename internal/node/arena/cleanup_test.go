package arena

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
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

func TestPrepareRestoreViewCleansPartialViewOnFailure(t *testing.T) {
	root := t.TempDir()
	m, err := NewManager(ManagerConfig{
		L2CASDir: root,
		ViewsDir: filepath.Join(root, "views"),
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := &protocol.SnapshotManifest{SnapshotID: "snap-1", CheckpointImgSize: 1}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, storage.ArtifactKey(manifest.SnapshotID, protocol.ManifestFileName))
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}

	if _, _, _, _, err := m.PrepareRestoreView(context.Background(), manifest.SnapshotID, "view-1"); err == nil {
		t.Fatal("restore unexpectedly succeeded without checkpoint artifact")
	}
	if _, err := os.Stat(filepath.Join(root, "views", "view-1")); !os.IsNotExist(err) {
		t.Fatalf("partial restore view remained after failure: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging", "view-1")); !os.IsNotExist(err) {
		t.Fatalf("partial restore staging remained after failure: %v", err)
	}
}

func TestManifestLoadRejectsSnapshotIDMismatch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalFSBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(ManagerConfig{L2CASDir: root, ViewsDir: filepath.Join(root, "views"), RemoteStore: backend})
	if err != nil {
		t.Fatal(err)
	}
	wrong := &protocol.SnapshotManifest{SnapshotID: "other"}
	raw, err := json.Marshal(wrong)
	if err != nil {
		t.Fatal(err)
	}
	localPath := filepath.Join(root, storage.ArtifactKey("requested", protocol.ManifestFileName))
	if err := os.MkdirAll(filepath.Dir(localPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(localPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ResolveManifest(ctx, "requested"); err == nil {
		t.Fatal("accepted a local manifest for a different snapshot")
	}
	if _, err := backend.PutManifest(ctx, "remote-requested", wrong); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ResolveManifest(ctx, "remote-requested"); err == nil {
		t.Fatal("accepted a remote manifest for a different snapshot")
	}
}

func TestRestartAdoptsOnlyOriginalMaterializedView(t *testing.T) {
	views := t.TempDir()
	path := filepath.Join(views, "view-one")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{protocol.GVisorCheckpointImg, protocol.GVisorPagesMetaImg, protocol.GVisorPagesImg} {
		if err := os.WriteFile(filepath.Join(path, name), []byte("data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	newManager := func() *Manager {
		return &Manager{cfg: ManagerConfig{ViewsDir: views}, activeViews: map[string]ViewCloser{}}
	}
	m := newManager()
	m.trackView(path, &plainDirHandle{dir: path})
	id, err := m.DescribeView(path)
	if err != nil {
		t.Fatal(err)
	}
	m.Shutdown()
	next := newManager()
	if err := next.AdoptView("view-one", path, id); err != nil {
		t.Fatal(err)
	}
	fuse := *id
	fuse.ArenaFS = true
	if err := next.AdoptView("view-one", path, &fuse); !errors.Is(err, ErrViewLost) {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(path, protocol.GVisorPagesImg), 0); err != nil {
		t.Fatal(err)
	}
	if err := newManager().AdoptView("view-one", path, id); !errors.Is(err, ErrViewLost) {
		t.Fatal(err)
	}
}

type retryUnmount struct{ attempts int }

func (v *retryUnmount) Close() error {
	v.attempts++
	if v.attempts == 1 {
		return errors.New("mount is busy")
	}
	return nil
}
func TestReleaseViewRetriesBusyUnmount(t *testing.T) {
	v := &retryUnmount{}
	m := &Manager{activeViews: map[string]ViewCloser{"view": v}}
	if err := m.ReleaseView("view"); err == nil {
		t.Fatal("expected unmount error")
	}
	if err := m.ReleaseView("view"); err != nil {
		t.Fatal(err)
	}
	if v.attempts != 2 || len(m.activeViews) != 0 {
		t.Fatalf("attempts=%d views=%d", v.attempts, len(m.activeViews))
	}
}

func TestConfirmReleaseRecognizesStagingAfterViewRemoval(t *testing.T) {
	root := t.TempDir()
	m := &Manager{cfg: ManagerConfig{ViewsDir: filepath.Join(root, "views"), L2CASDir: root}}
	path := filepath.Join(m.cfg.ViewsDir, "view-one")
	stage := filepath.Join(root, "staging", "view-one")
	if err := os.MkdirAll(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmReleaseView("view-one", path, &ViewIdentity{}); err == nil {
		t.Fatal("removed unconfirmed staging")
	}
	id, err := m.stagingIdentity("view-one")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := m.ConfirmReleaseView("view-one", path, &ViewIdentity{Staging: id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}
