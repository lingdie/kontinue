package arena

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

type checkpointBlob struct {
	key              string
	hash             string
	size             int64
	contentAddressed bool
}

func checkpointArtifacts(mf *protocol.SnapshotManifest) []checkpointBlob {
	var artifacts []checkpointBlob
	for _, art := range []checkpointBlob{
		{key: protocol.GVisorCheckpointImg, hash: mf.CheckpointImgHash, size: mf.CheckpointImgSize},
		{key: protocol.GVisorPagesMetaImg, hash: mf.PagesMetaImgHash, size: mf.PagesMetaImgSize},
		{key: protocol.RootfsUpperTar, hash: mf.RootfsUpperTarHash, size: mf.RootfsUpperTarSize},
	} {
		if art.hash != "" || art.size != 0 {
			art.key = storage.ArtifactKey(mf.SnapshotID, art.key)
			artifacts = append(artifacts, art)
		}
	}
	return artifacts
}

func (m *Manager) CheckpointManifestExists(ctx context.Context, snapshotID string) (bool, error) {
	key := storage.ArtifactKey(snapshotID, protocol.ManifestFileName)
	if _, err := os.Lstat(filepath.Join(m.cfg.L2CASDir, key)); !errors.Is(err, os.ErrNotExist) {
		return err == nil, err
	}
	if m.cfg.RemoteStore != nil {
		return m.cfg.RemoteStore.HasBlob(ctx, key)
	}
	return false, nil
}

func (m *Manager) checkpointManifestURI(snapshotID string) string {
	if m.cfg.RemoteStore != nil {
		return m.cfg.RemoteStore.ManifestURI(snapshotID)
	}
	return "local://" + filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(snapshotID, protocol.ManifestFileName))
}

func (m *Manager) writeCheckpointManifest(mf *protocol.SnapshotManifest) error {
	raw, err := json.Marshal(mf)
	if err != nil {
		return err
	}
	path := filepath.Join(m.cfg.L2CASDir, storage.ArtifactKey(mf.SnapshotID, protocol.ManifestFileName))
	if err := writeAtomicFile(path, raw); err != nil {
		return err
	}
	m.mu.Lock()
	m.l2Manifests[mf.SnapshotID] = mf
	m.mu.Unlock()
	return nil
}

func (m *Manager) publishCheckpointManifest(ctx context.Context, mf *protocol.SnapshotManifest) error {
	if err := m.writeCheckpointManifest(mf); err != nil {
		return err
	}
	if m.cfg.RemoteStore != nil {
		_, err := m.uploadSnapshotToRemote(ctx, mf)
		return err
	}
	return nil
}

// RecoverCheckpoint verifies the manifest against the owner's staged result.
// The remote manifest is the last write, after all referenced blobs are uploaded.
func (m *Manager) RecoverCheckpoint(ctx context.Context, mf *protocol.SnapshotManifest, uri string) error {
	if uri != m.checkpointManifestURI(mf.SnapshotID) {
		return fmt.Errorf("storage location for checkpoint %s changed since staging", mf.SnapshotID)
	}
	local, err := m.LoadLocalManifest(mf.SnapshotID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if local != nil && !sameCheckpointManifest(local, mf) {
		return fmt.Errorf("local manifest for %s differs from the staged checkpoint", mf.SnapshotID)
	}
	if m.cfg.RemoteStore != nil {
		remote, err := m.cfg.RemoteStore.GetManifest(ctx, mf.SnapshotID)
		if err == nil {
			if !sameCheckpointManifest(remote, mf) {
				return fmt.Errorf("remote manifest for %s differs from the staged checkpoint", mf.SnapshotID)
			}
			if err := m.verifyCheckpointBlobs(ctx, mf, true); err != nil {
				return err
			}
			if local == nil {
				return m.writeCheckpointManifest(mf)
			}
			return nil
		}
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return err
		}
	}
	if err := m.verifyCheckpointBlobs(ctx, mf, false); err != nil {
		return err
	}
	if local != nil {
		if m.cfg.RemoteStore == nil {
			return nil
		}
		_, err := m.uploadSnapshotToRemote(ctx, mf)
		return err
	}
	return m.publishCheckpointManifest(ctx, mf)
}

func sameCheckpointManifest(a, b *protocol.SnapshotManifest) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

func (m *Manager) verifyCheckpointBlobs(ctx context.Context, mf *protocol.SnapshotManifest, remote bool) error {
	if mf.IncludeMemory && (mf.CheckpointImgHash == "" || mf.PagesMetaImgHash == "") {
		return fmt.Errorf("checkpoint %s is missing its core artifacts", mf.SnapshotID)
	}
	blobs := checkpointArtifacts(mf)
	var end int64
	for _, chunk := range mf.Chunks {
		if chunk.Offset != end || chunk.Length <= 0 {
			return fmt.Errorf("incomplete page map for checkpoint %s", mf.SnapshotID)
		}
		end += int64(chunk.Length)
		if !chunk.ZeroChunk {
			blobs = append(blobs, checkpointBlob{key: storage.ChunkKey(chunk.Blake3Hash), hash: chunk.Blake3Hash, size: int64(chunk.Length), contentAddressed: true})
		}
	}
	if end != mf.PagesImgTotalSize {
		return fmt.Errorf("incomplete page map for checkpoint %s", mf.SnapshotID)
	}
	checks, ctx := errgroup.WithContext(ctx)
	checks.SetLimit(16)
	seen := make(map[string]bool, len(blobs))
	for _, blob := range blobs {
		if seen[blob.key] {
			continue
		}
		seen[blob.key] = true
		checks.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if blob.hash == "" {
				return fmt.Errorf("checkpoint does not describe %s", blob.key)
			}
			if remote {
				return m.verifyRemoteCheckpointBlob(ctx, blob)
			}
			return verifyLocalCheckpointBlob(filepath.Join(m.cfg.L2CASDir, blob.key), blob)
		})
	}
	return checks.Wait()
}

func (m *Manager) verifyRemoteCheckpointBlob(ctx context.Context, blob checkpointBlob) error {
	if blob.contentAddressed {
		exists, err := m.cfg.RemoteStore.HasBlob(ctx, blob.key)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("checkpoint blob %s is missing from remote storage", blob.key)
		}
		return nil
	}
	data, err := m.cfg.RemoteStore.GetBlob(ctx, blob.key)
	if err != nil {
		return err
	}
	if int64(len(data)) != blob.size || fmt.Sprintf("%x", blake3.Sum256(data)) != blob.hash {
		return fmt.Errorf("checkpoint blob %s does not match the staged manifest", blob.key)
	}
	return nil
}

func verifyLocalCheckpointBlob(path string, blob checkpointBlob) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	hash := blake3.New(32, nil)
	n, err := io.Copy(hash, f)
	if err != nil {
		return err
	}
	if n != blob.size || fmt.Sprintf("%x", hash.Sum(nil)) != blob.hash {
		return fmt.Errorf("checkpoint blob %s does not match the staged manifest", blob.key)
	}
	return nil
}
