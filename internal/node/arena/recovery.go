package arena

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

// A materialized view can outlive its daemon. An ArenaFS mount depends on the
// original daemon's FUSE server even if its mountpoint still exists.
type ViewIdentity struct {
	Staging    FileIdentity    `json:"staging"`
	ArenaFS    bool            `json:"arenaFs"`
	Directory  FileIdentity    `json:"directory"`
	Mountpoint FileIdentity    `json:"mountpoint,omitzero"`
	Artifacts  [3]FileIdentity `json:"artifacts"`
}

type FileIdentity struct {
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	ModifiedNS int64  `json:"modifiedNs,omitempty"`
	Size       int64  `json:"size,omitempty"`
}

var ErrViewLost = errors.New("restore view is unavailable")

func inspectView(path string) (ViewIdentity, error) {
	var identity ViewIdentity
	for i, name := range []string{"", protocol.GVisorCheckpointImg, protocol.GVisorPagesMetaImg, protocol.GVisorPagesImg} {
		info, err := os.Lstat(filepath.Join(path, name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTCONN) || errors.Is(err, syscall.ESTALE) {
				return identity, fmt.Errorf("%w: %v", ErrViewLost, err)
			}
			return identity, err
		}
		stat := info.Sys().(*syscall.Stat_t)
		file := FileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
		if i == 0 && info.IsDir() {
			identity.Directory = file
		} else if i > 0 && info.Mode().IsRegular() {
			file.Size = info.Size()
			file.ModifiedNS = info.ModTime().UnixNano()
			f, err := os.Open(filepath.Join(path, name))
			if err != nil {
				return identity, err
			}
			err = f.Close()
			if err != nil {
				return identity, err
			}
			identity.Artifacts[i-1] = file
		} else {
			return identity, fmt.Errorf("%w: unexpected file type for %s", ErrViewLost, filepath.Join(path, name))
		}
	}
	return identity, nil
}

func (m *Manager) DescribeView(path string) (*ViewIdentity, error) {
	m.mu.RLock()
	h := m.activeViews[path]
	m.mu.RUnlock()
	if h == nil {
		return nil, fmt.Errorf("unregistered view %s", path)
	}
	id, err := inspectView(path)
	if err != nil {
		return nil, err
	}
	_, plain := h.(*plainDirHandle)
	id.ArenaFS = !plain
	if mounted, ok := h.(interface{ mountpointIdentity() FileIdentity }); ok {
		id.Mountpoint = mounted.mountpointIdentity()
	}
	id.Staging, err = m.stagingIdentity(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (m *Manager) validViewPath(id, path string) bool {
	if !filepath.IsLocal(id) || filepath.Base(id) != id || id == "." {
		return false
	}
	return filepath.Join(m.cfg.ViewsDir, id) == path || (m.cfg.L1TmpfsDir != "" && filepath.Join(m.cfg.L1TmpfsDir, ".views", id) == path)
}

func (m *Manager) AdoptView(viewID, path string, expected *ViewIdentity) error {
	if expected == nil || !m.validViewPath(viewID, path) {
		return fmt.Errorf("unconfirmed restore view %q", path)
	}
	if expected.ArenaFS {
		return fmt.Errorf("%w: ArenaFS serving process exited", ErrViewLost)
	}
	current, err := inspectView(path)
	if err != nil {
		return err
	}
	// Version-one journals originally stored device, inode and length.
	for i := range current.Artifacts {
		if expected.Artifacts[i].ModifiedNS == 0 {
			current.Artifacts[i].ModifiedNS = 0
		}
	}
	current.Staging = expected.Staging // A materialized view owns its page file independently of staging.
	if current != *expected {
		return fmt.Errorf("%w: materialized restore artifacts changed", ErrViewLost)
	}
	m.trackView(path, &plainDirHandle{dir: path})
	return nil
}

// ConfirmReleaseView also handles a crash after unlinking the materialized
// directory but before removing its ownership record. Unknown staging remains
// reserved for inspection, including partially prepared views.
func (m *Manager) ConfirmReleaseView(viewID, path string, expected *ViewIdentity) error {
	if filepath.Base(viewID) != viewID || !filepath.IsLocal(viewID) || viewID == "." {
		return fmt.Errorf("invalid view ID %q", viewID)
	}
	claimed := filepath.Join(m.cfg.ViewsDir, viewID)
	if path != "" {
		if !m.validViewPath(viewID, path) {
			return fmt.Errorf("unexpected view path %q", path)
		}
		claimed = path
	} else if m.cfg.L1TmpfsDir != "" {
		if _, err := os.Lstat(filepath.Join(m.cfg.L1TmpfsDir, ".views", viewID)); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("unconfirmed tmpfs view %s", viewID)
		}
	}
	actual, err := m.stagingIdentity(viewID)
	if err != nil {
		return err
	}
	if actual != (FileIdentity{}) && (expected == nil || actual != expected.Staging) {
		return fmt.Errorf("unconfirmed staging remains for view %s", viewID)
	}
	if _, err := os.Lstat(claimed); errors.Is(err, os.ErrNotExist) {
		// Cleanup of the view completed before the previous process exited.
	} else if expected != nil && expected.ArenaFS {
		if err := releaseDisconnectedArenaFS(claimed, expected, err); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if err := m.AdoptView(viewID, claimed, expected); err != nil {
			return err
		}
		if err := m.ReleaseView(claimed); err != nil {
			return err
		}
	}
	if actual == (FileIdentity{}) {
		return nil
	}
	return os.RemoveAll(filepath.Join(m.cfg.L2CASDir, "staging", viewID))
}

func (m *Manager) stagingIdentity(viewID string) (FileIdentity, error) {
	return directoryIdentity(filepath.Join(m.cfg.L2CASDir, "staging", viewID))
}

func directoryIdentity(path string) (FileIdentity, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return FileIdentity{}, nil
	}
	if err != nil {
		return FileIdentity{}, err
	}
	if !info.IsDir() {
		return FileIdentity{}, fmt.Errorf("expected a directory at %s", path)
	}
	stat := info.Sys().(*syscall.Stat_t)
	return FileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}
