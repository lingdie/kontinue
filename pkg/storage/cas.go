package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

var ErrObjectNotFound = errors.New("storage object not found")

// Backend abstracts shared durable storage (S3 / MinIO / Shared FS) for SnapshotManifests,
// 2MiB Blake3 memory chunks, checkpoint.img, pages_meta.img, and rootfs diffs.
type Backend interface {
	ManifestURI(snapshotID string) string
	PutManifest(ctx context.Context, snapshotID string, manifest *protocol.SnapshotManifest) (string, error)
	GetManifest(ctx context.Context, snapshotID string) (*protocol.SnapshotManifest, error)

	HasBlob(ctx context.Context, key string) (bool, error)
	PutBlob(ctx context.Context, key string, r io.Reader, size int64) error
	GetBlob(ctx context.Context, key string) ([]byte, error)
	GetBlobRange(ctx context.Context, key string, offset int64, length int64) ([]byte, error)
	DeleteBlob(ctx context.Context, key string) error
}

func ChunkKey(blake3Hash string) string {
	if len(blake3Hash) >= 4 {
		return fmt.Sprintf("cas/chunks/%s/%s.bin", blake3Hash[:4], blake3Hash)
	}
	return fmt.Sprintf("cas/chunks/misc/%s.bin", blake3Hash)
}

func ArtifactKey(snapshotID, name string) string {
	return fmt.Sprintf("snapshots/%s/%s", snapshotID, name)
}

// LocalFSBackend stores CAS blobs and manifests atomically on a local or shared filesystem.
type LocalFSBackend struct {
	root string
}

func NewLocalFSBackend(root string) (*LocalFSBackend, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create storage root %s: %w", root, err)
	}
	return &LocalFSBackend{root: root}, nil
}

func (b *LocalFSBackend) Root() string { return b.root }

func (b *LocalFSBackend) safePath(key string) (string, error) {
	cleaned := filepath.Clean("/" + key)
	rel := strings.TrimPrefix(cleaned, "/")
	full := filepath.Join(b.root, rel)
	if !strings.HasPrefix(full, filepath.Clean(b.root)+string(os.PathSeparator)) && full != filepath.Clean(b.root) {
		return "", fmt.Errorf("invalid storage key %q", key)
	}
	return full, nil
}

func (b *LocalFSBackend) ManifestURI(snapshotID string) string {
	return "file://" + ArtifactKey(snapshotID, protocol.ManifestFileName)
}

func (b *LocalFSBackend) PutManifest(ctx context.Context, snapshotID string, manifest *protocol.SnapshotManifest) (string, error) {
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	key := ArtifactKey(snapshotID, protocol.ManifestFileName)
	if err := b.PutBlob(ctx, key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		return "", err
	}
	return b.ManifestURI(snapshotID), nil
}

func (b *LocalFSBackend) GetManifest(ctx context.Context, snapshotID string) (*protocol.SnapshotManifest, error) {
	key := ArtifactKey(snapshotID, protocol.ManifestFileName)
	raw, err := b.GetBlob(ctx, key)
	if err != nil {
		return nil, err
	}
	var m protocol.SnapshotManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", snapshotID, err)
	}
	return &m, nil
}

func (b *LocalFSBackend) HasBlob(_ context.Context, key string) (bool, error) {
	p, err := b.safePath(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(p)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (b *LocalFSBackend) PutBlob(_ context.Context, key string, r io.Reader, _ int64) error {
	p, err := b.safePath(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", p, time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, p)
}

func (b *LocalFSBackend) GetBlob(_ context.Context, key string) ([]byte, error) {
	p, err := b.safePath(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	return data, err
}

func (b *LocalFSBackend) GetBlobRange(_ context.Context, key string, offset int64, length int64) ([]byte, error) {
	p, err := b.safePath(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func (b *LocalFSBackend) DeleteBlob(_ context.Context, key string) error {
	p, err := b.safePath(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ListLocalChunkHashes walks cas/chunks/**/*.bin under a LocalFSBackend root.
func (b *LocalFSBackend) ListLocalChunkHashes() ([]string, error) {
	root := filepath.Join(b.root, "cas", "chunks")
	var out []string
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".bin") {
			return nil
		}
		hash := strings.TrimSuffix(d.Name(), ".bin")
		if hash != "" {
			out = append(out, hash)
		}
		return nil
	})
	return out, nil
}

// HTTPObjectStoreBackend implements Backend over S3/MinIO HTTP endpoints with HTTP Range support.
type HTTPObjectStoreBackend struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewHTTPObjectStoreBackend(baseURL, bearerToken string, client *http.Client) *HTTPObjectStoreBackend {
	if client == nil {
		var tr http.RoundTripper = http.DefaultTransport
		if protocol.DefaultHTTPTransportWrapper != nil {
			tr = protocol.DefaultHTTPTransportWrapper("cas-http-client", tr)
		}
		client = &http.Client{Timeout: 60 * time.Second, Transport: tr}
	}
	return &HTTPObjectStoreBackend{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   bearerToken,
		client:  client,
	}
}

func (h *HTTPObjectStoreBackend) urlFor(key string) string {
	return h.baseURL + "/" + strings.TrimLeft(key, "/")
}

func (h *HTTPObjectStoreBackend) authorize(req *http.Request) {
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
}

func (h *HTTPObjectStoreBackend) ManifestURI(snapshotID string) string {
	return h.urlFor(ArtifactKey(snapshotID, protocol.ManifestFileName))
}

func (h *HTTPObjectStoreBackend) PutManifest(ctx context.Context, snapshotID string, manifest *protocol.SnapshotManifest) (string, error) {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	key := ArtifactKey(snapshotID, protocol.ManifestFileName)
	if err := h.PutBlob(ctx, key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		return "", err
	}
	return h.ManifestURI(snapshotID), nil
}

func (h *HTTPObjectStoreBackend) GetManifest(ctx context.Context, snapshotID string) (*protocol.SnapshotManifest, error) {
	key := ArtifactKey(snapshotID, protocol.ManifestFileName)
	raw, err := h.GetBlob(ctx, key)
	if err != nil {
		return nil, err
	}
	var m protocol.SnapshotManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (h *HTTPObjectStoreBackend) HasBlob(ctx context.Context, key string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, h.urlFor(key), nil)
	if err != nil {
		return false, err
	}
	h.authorize(req)
	resp, err := h.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("HEAD %s status %d", key, resp.StatusCode)
}

func (h *HTTPObjectStoreBackend) PutBlob(ctx context.Context, key string, r io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, h.urlFor(key), r)
	if err != nil {
		return err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	h.authorize(req)
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("PUT %s failed (%d): %s", key, resp.StatusCode, string(body))
}

func (h *HTTPObjectStoreBackend) GetBlob(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.urlFor(key), nil)
	if err != nil {
		return nil, err
	}
	h.authorize(req)
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s failed (%d)", key, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (h *HTTPObjectStoreBackend) GetBlobRange(ctx context.Context, key string, offset int64, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.urlFor(key), nil)
	if err != nil {
		return nil, err
	}
	h.authorize(req)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Range GET %s failed (%d)", key, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK && int64(len(data)) > offset {
		end := offset + length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		return data[offset:end], nil
	}
	return data, nil
}

func (h *HTTPObjectStoreBackend) DeleteBlob(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, h.urlFor(key), nil)
	if err != nil {
		return err
	}
	h.authorize(req)
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("DELETE %s failed (%d): %s", key, resp.StatusCode, string(body))
}
