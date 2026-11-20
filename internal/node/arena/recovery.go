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
	ArenaFS   bool            `json:"arenaFs"`
	Directory FileIdentity    `json:"directory"`
	Artifacts [3]FileIdentity `json:"artifacts"`
}

type FileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size,omitempty"`
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
	return &id, nil
}

func (m *Manager) AdoptView(viewID, path string, expected *ViewIdentity) error {
	if expected == nil || !filepath.IsLocal(viewID) || filepath.Base(viewID) != viewID || filepath.Join(m.cfg.ViewsDir, viewID) != path {
		return fmt.Errorf("unconfirmed restore view %q", path)
	}
	if expected.ArenaFS {
		return fmt.Errorf("%w: ArenaFS serving process exited", ErrViewLost)
	}
	current, err := inspectView(path)
	if err != nil {
		return err
	}
	if current != *expected {
		return fmt.Errorf("%w: materialized restore artifacts changed", ErrViewLost)
	}
	m.trackView(path, &plainDirHandle{dir: path})
	return nil
}
