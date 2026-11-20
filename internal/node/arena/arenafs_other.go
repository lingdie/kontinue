//go:build !linux

package arena

import "context"

func mountArenaFSView(ctx context.Context, mountDir string, ckptBytes, metaBytes []byte, reader *LazyPageReader) (ViewCloser, error) {
	if err := fallbackMaterializeDir(ctx, mountDir, ckptBytes, metaBytes, reader); err != nil {
		return nil, err
	}
	return &plainDirHandle{dir: mountDir, reader: reader}, nil
}
