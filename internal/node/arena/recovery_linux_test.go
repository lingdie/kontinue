package arena

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReleaseAfterArenaFSUnmountChecksCoveredDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "view-one")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	covered, err := directoryIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := &ViewIdentity{ArenaFS: true, Mountpoint: covered}
	m := &Manager{cfg: ManagerConfig{ViewsDir: root, L2CASDir: root}}
	if err := os.Rename(path, path+"-original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmReleaseView("view-one", path, expected); err == nil {
		t.Fatal("accepted a replacement mountpoint")
	}
	if err := m.ConfirmReleaseView("view-one", path, &ViewIdentity{ArenaFS: true}); err == nil {
		t.Fatal("accepted an unidentified directory from an older journal")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+"-original", path); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := m.ConfirmReleaseView("view-one", path, expected); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original mountpoint still present: %v", err)
	}
}
