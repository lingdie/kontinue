//go:build linux

package arena

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

type fuseViewHandle struct {
	server     *fuse.Server
	reader     *LazyPageReader
	mountDir   string
	staging    string
	mountpoint FileIdentity
}

func (h *fuseViewHandle) mountpointIdentity() FileIdentity { return h.mountpoint }

// Called only on explicit, fenced release after the owning runsc has stopped.
// A disconnected FUSE root rejects lstat, but mountinfo still identifies its superblock.
func releaseDisconnectedArenaFS(path string, expected *ViewIdentity, statErr error) error {
	disconnected := errors.Is(statErr, syscall.ENOTCONN)
	if statErr != nil && !disconnected {
		return statErr
	}
	if disconnected {
		mounts, err := mountinfo.GetMounts(func(m *mountinfo.Info) (bool, bool) {
			return m.Mountpoint != path, false
		})
		if err != nil {
			return err
		}
		if len(mounts) != 1 {
			return fmt.Errorf("cannot identify a single ArenaFS mount at %s", path)
		}
		m := mounts[0]
		if m.FSType != "fuse.arenafs" || m.Source != "kontinue-arenafs" || m.Root != "/" ||
			unix.Mkdev(uint32(m.Major), uint32(m.Minor)) != expected.Directory.Device {
			return fmt.Errorf("ArenaFS mount identity changed at %s", path)
		}
		if err := unix.Unmount(path, unix.UMOUNT_NOFOLLOW); err != nil {
			return fmt.Errorf("unmount disconnected ArenaFS %s: %w", path, err)
		}
	}
	// Older journals lack the covered directory's inode. Only remove their empty
	// mountpoint in the same call that unmounted the verified filesystem.
	if expected.Mountpoint != (FileIdentity{}) || !disconnected {
		actual, err := directoryIdentity(path)
		if err != nil {
			return err
		}
		if expected.Mountpoint == (FileIdentity{}) || actual != expected.Mountpoint {
			return fmt.Errorf("unconfirmed ArenaFS mountpoint at %s", path)
		}
	}
	return unix.Rmdir(path)
}

func (h *fuseViewHandle) RecordedFaults() []uint32 {
	if h.reader == nil {
		return nil
	}
	return h.reader.RecordedHotPages()
}

func (h *fuseViewHandle) SnapshotID() string {
	if h.reader == nil {
		return ""
	}
	return h.reader.SnapshotID()
}

func (h *fuseViewHandle) Close() error {
	if h.server != nil {
		if err := h.server.Unmount(); err != nil {
			return err
		}
		h.server = nil
	}
	if h.reader != nil {
		if err := h.reader.Close(); err != nil {
			return err
		}
	}
	if h.staging != "" {
		if err := os.RemoveAll(h.staging); err != nil {
			return err
		}
	}
	if h.mountDir != "" {
		return os.RemoveAll(h.mountDir)
	}
	return nil
}

type arenaFSRoot struct {
	fs.Inode
	checkpointBytes []byte
	pagesMetaBytes  []byte
	lazyPages       *LazyPageReader
}

var _ = (fs.NodeOnAdder)((*arenaFSRoot)(nil))

func (r *arenaFSRoot) OnAdd(ctx context.Context) {
	ckptNode := &fs.MemRegularFile{
		Data: r.checkpointBytes,
		Attr: fuse.Attr{Mode: 0o400},
	}
	r.AddChild(protocol.GVisorCheckpointImg, r.NewPersistentInode(ctx, ckptNode, fs.StableAttr{Mode: syscall.S_IFREG}), false)

	metaNode := &fs.MemRegularFile{
		Data: r.pagesMetaBytes,
		Attr: fuse.Attr{Mode: 0o400},
	}
	r.AddChild(protocol.GVisorPagesMetaImg, r.NewPersistentInode(ctx, metaNode, fs.StableAttr{Mode: syscall.S_IFREG}), false)

	if r.lazyPages != nil {
		pagesNode := &lazyPagesInode{reader: r.lazyPages}
		r.AddChild(protocol.GVisorPagesImg, r.NewPersistentInode(ctx, pagesNode, fs.StableAttr{Mode: syscall.S_IFREG}), false)
	}
}

type lazyPagesInode struct {
	fs.Inode
	reader *LazyPageReader
}

var _ = (fs.NodeGetattrer)((*lazyPagesInode)(nil))
var _ = (fs.NodeOpener)((*lazyPagesInode)(nil))
var _ = (fs.NodeReader)((*lazyPagesInode)(nil))

func (n *lazyPagesInode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0o400
	out.Size = uint64(n.reader.Size())
	return 0
}

func (n *lazyPagesInode) Open(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *lazyPagesInode) Read(ctx context.Context, _ fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	readN, err := n.reader.ReadAt(ctx, dest, off)
	if err != nil && readN == 0 {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:readN]), 0
}

func mountArenaFSView(ctx context.Context, mountDir string, ckptBytes, metaBytes []byte, reader *LazyPageReader) (ViewCloser, error) {
	if err := os.MkdirAll(mountDir, 0o700); err != nil {
		return nil, err
	}
	mountpoint, err := directoryIdentity(mountDir)
	if err != nil {
		return nil, err
	}
	root := &arenaFSRoot{
		checkpointBytes: ckptBytes,
		pagesMetaBytes:  metaBytes,
		lazyPages:       reader,
	}
	srv, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:  "kontinue-arenafs",
			Name:    "arenafs",
			Options: []string{"ro"},
		},
	})
	if err != nil {
		// Graceful fallback inside unprivileged containers lacking /dev/fuse: materialize directly.
		if wErr := fallbackMaterializeDir(ctx, mountDir, ckptBytes, metaBytes, reader); wErr != nil {
			return nil, err
		}
		return &plainDirHandle{dir: mountDir, reader: reader}, nil
	}
	reader.StartHotPagePrefetcher(context.Background(), 4)
	return &fuseViewHandle{
		server:     srv,
		reader:     reader,
		mountDir:   mountDir,
		mountpoint: mountpoint,
		staging:    filepath.Dir(reader.sparseFile.Name()),
	}, nil
}
