//go:build linux

package arena

import (
	"context"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type fuseViewHandle struct {
	server   *fuse.Server
	reader   *LazyPageReader
	mountDir string
	staging  string
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
	var err error
	if h.server != nil {
		err = h.server.Unmount()
	}
	if h.reader != nil {
		_ = h.reader.Close()
	}
	if h.staging != "" {
		_ = os.RemoveAll(h.staging)
	}
	if h.mountDir != "" {
		_ = os.RemoveAll(h.mountDir)
	}
	return err
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
		server:   srv,
		reader:   reader,
		mountDir: mountDir,
		staging:  filepath.Dir(reader.sparseFile.Name()),
	}, nil
}
