package hydrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Simulate ctr's exact-reference lookup and materialization using small files.
// A mutable tag must produce a new root without changing a live guest's old one.
func TestRootfsCacheFollowsImageDigest(t *testing.T) {
	dir := t.TempDir()
	ctr := filepath.Join(dir, "ctr")
	digestFile := filepath.Join(dir, "digest")
	t.Setenv("TEST_CTR_DIGEST", digestFile)
	script := `#!/bin/sh
set -eu
case "$4" in
  ls)
    test "$5" = 'name=="example:local"'
    printf 'example:local manifest %s\n' "$(cat "$TEST_CTR_DIGEST")"
    ;;
  mount) cp "$TEST_CTR_DIGEST" "$7/content" ;;
  unmount) exit 0 ;;
  *) exit 1 ;;
esac
`
	if err := os.WriteFile(ctr, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	driver := &GVisorSandboxDriver{cfg: GVisorDriverConfig{BaseRootfsCache: filepath.Join(dir, "cache"), CtrBinary: ctr}}
	stage := func(digest string) string {
		t.Helper()
		if err := os.WriteFile(digestFile, []byte(digest), 0600); err != nil {
			t.Fatal(err)
		}
		path, err := driver.ensureBaseRootfs(context.Background(), "example:local")
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	first := stage("sha256:first")
	if replay := stage("sha256:first"); replay != first {
		t.Fatalf("same digest changed rootfs: %s", replay)
	}
	second := stage("sha256:second")
	if first == second {
		t.Fatal("retag reused prior rootfs")
	}
	for root, want := range map[string]string{first: "sha256:first", second: "sha256:second"} {
		b, err := os.ReadFile(filepath.Join(root, "content"))
		if err != nil || string(b) != want {
			t.Fatalf("%s content %q: %v", root, b, err)
		}
	}
	if err := os.Remove(digestFile); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.ensureBaseRootfs(context.Background(), "example:local"); err == nil {
		t.Fatal("unresolved tag reused cached rootfs")
	}
}

func TestRootfsMountIsPreservedUntilUnmountSucceeds(t *testing.T) {
	dir := t.TempDir()
	ctr := filepath.Join(dir, "ctr")
	busy := filepath.Join(dir, "busy")
	t.Setenv("TEST_CTR_BUSY", busy)
	if err := os.WriteFile(busy, nil, 0600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
set -eu
case "$4" in
  ls) printf 'example:local manifest sha256:layer\n' ;;
  mount) printf 'mounted image contents\n' > "$7/content" ;;
  unmount) if test -f "$TEST_CTR_BUSY"; then echo 'mount still busy' >&2; exit 1; fi ;;
  *) exit 1 ;;
esac
`
	if err := os.WriteFile(ctr, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(dir, "cache")
	driver := &GVisorSandboxDriver{cfg: GVisorDriverConfig{BaseRootfsCache: cache, CtrBinary: ctr}}
	for range 2 {
		if _, err := driver.ensureBaseRootfs(context.Background(), "example:local"); err == nil || !strings.Contains(err.Error(), "mount still busy") {
			t.Fatalf("unconfirmed unmount was treated as success: %v", err)
		}
		paths, err := filepath.Glob(filepath.Join(cache, "*", "mnt", "content"))
		if err != nil || len(paths) != 1 {
			t.Fatalf("mount data was removed: %v %v", paths, err)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(filepath.Dir(paths[0])), ".ready")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("busy stage was advertised as ready: %v", err)
		}
	}
	if err := os.Remove(busy); err != nil {
		t.Fatal(err)
	}
	root, err := driver.ensureBaseRootfs(context.Background(), "example:local")
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "content")); err != nil || string(data) != "mounted image contents\n" {
		t.Fatalf("rootfs did not recover: data=%q err=%v", data, err)
	}
}
