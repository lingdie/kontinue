//go:build !linux

package arena

import (
	"context"
	"fmt"
)

func releaseDisconnectedArenaFS(path string, _ *ViewIdentity, _ error) error {
	return fmt.Errorf("cannot release a recovered Linux ArenaFS mount at %s", path)
}

func mountArenaFSView(ctx context.Context, mountDir string, ckptBytes, metaBytes []byte, reader *LazyPageReader) (ViewCloser, error) {
	if err := fallbackMaterializeDir(ctx, mountDir, ckptBytes, metaBytes, reader); err != nil {
		return nil, err
	}
	return &plainDirHandle{dir: mountDir, reader: reader}, nil
}
