package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaDDL = `
CREATE TABLE IF NOT EXISTS kt_templates (
    template_id        TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    base_image         TEXT NOT NULL,
    warmup_script      TEXT NOT NULL DEFAULT '',
    tier               TEXT NOT NULL DEFAULT 'default',
    pin_in_l1_tmpfs    BOOLEAN NOT NULL DEFAULT TRUE,
    trace_hot_pages    BOOLEAN NOT NULL DEFAULT TRUE,
    phase              TEXT NOT NULL,
    golden_snapshot_id TEXT NOT NULL DEFAULT '',
    status_message     TEXT NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS kt_snapshots (
    snapshot_id        TEXT PRIMARY KEY,
    kind               TEXT NOT NULL,
    template_id        TEXT NOT NULL DEFAULT '',
    source_sandbox_id  TEXT NOT NULL DEFAULT '',
    parent_snapshot_id TEXT NOT NULL DEFAULT '',
    base_image         TEXT NOT NULL,
    manifest_uri       TEXT NOT NULL,
    origin_node_id     TEXT NOT NULL,
    origin_peer_addr   TEXT NOT NULL DEFAULT '',
    total_memory_bytes BIGINT NOT NULL DEFAULT 0,
    dirty_memory_bytes BIGINT NOT NULL DEFAULT 0,
    dirty_chunks       INT NOT NULL DEFAULT 0,
    reused_chunks      INT NOT NULL DEFAULT 0,
    superseded         BOOLEAN NOT NULL DEFAULT FALSE,
    bootstrap_token    TEXT NOT NULL DEFAULT '',
    fingerprint_json   JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS kt_sandboxes (
    sandbox_id            TEXT PRIMARY KEY,
    payload_json          JSONB NOT NULL,
    state                 TEXT NOT NULL,
    active_snapshot_id    TEXT NOT NULL DEFAULT '',
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kt_sandboxes_state ON kt_sandboxes(state);
CREATE INDEX IF NOT EXISTS idx_kt_snapshots_superseded ON kt_snapshots(superseded);
`

// PostgresMetadataStore implements MetadataStore backed by PostgreSQL (pgxpool).
type PostgresMetadataStore struct {
	pool          *pgxpool.Pool
	locks         *pgxpool.Pool
	snapshotLocks *pgxpool.Pool
}

func NewPostgresMetadataStore(ctx context.Context, connString string) (*PostgresMetadataStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	// Keep lock holders from exhausting the connections used for metadata writes.
	cfg := pool.Config()
	cfg.MaxConns = max(cfg.MaxConns, 4)
	locks, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		pool.Close()
		return nil, err
	}
	// Publishers can already hold a lifecycle lock while waiting for GC.
	snapshotLocks, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		locks.Close()
		pool.Close()
		return nil, err
	}
	return &PostgresMetadataStore{pool: pool, locks: locks, snapshotLocks: snapshotLocks}, nil
}

func (s *PostgresMetadataStore) Close() { s.snapshotLocks.Close(); s.locks.Close(); s.pool.Close() }

func (s *PostgresMetadataStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaDDL)
	return err
}

func advisoryKey(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

// TryLeader holds a session-scoped pg_try_advisory_lock on a dedicated connection
// so multi-replica apiservers do not double-run replenish / eviction sweeps.
func (s *PostgresMetadataStore) TryLeader(ctx context.Context, name string) (func(), bool, error) {
	conn, err := s.locks.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, advisoryKey(name)).Scan(&ok); err != nil {
		_ = conn.Conn().Close(context.Background())
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return func() {}, false, nil
	}
	released := false
	return func() {
		if released {
			return
		}
		released = true
		if _, err := conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey(name)); err != nil {
			_ = conn.Conn().Close(context.Background())
		}
		conn.Release()
	}, true, nil
}

func (s *PostgresMetadataStore) CreateTemplate(ctx context.Context, tpl *TemplateRecord) error {
	now := time.Now().UTC()
	if tpl.CreatedAt.IsZero() {
		tpl.CreatedAt = now
	}
	tpl.UpdatedAt = now
	_, err := s.pool.Exec(ctx, `
		INSERT INTO kt_templates (
			template_id, name, base_image, warmup_script, tier, pin_in_l1_tmpfs,
			trace_hot_pages, phase, golden_snapshot_id, status_message, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		tpl.TemplateID, tpl.Name, tpl.BaseImage, tpl.WarmupScript, tpl.Tier, tpl.PinInL1Tmpfs,
		tpl.TraceHotPages, tpl.Phase, tpl.GoldenSnapshotID, tpl.StatusMessage, tpl.CreatedAt, tpl.UpdatedAt,
	)
	return err
}

func (s *PostgresMetadataStore) GetTemplate(ctx context.Context, templateID string) (*TemplateRecord, error) {
	var t TemplateRecord
	err := s.pool.QueryRow(ctx, `
		SELECT template_id, name, base_image, warmup_script, tier, pin_in_l1_tmpfs,
		       trace_hot_pages, phase, golden_snapshot_id, status_message, created_at, updated_at
		FROM kt_templates WHERE template_id = $1`, templateID,
	).Scan(
		&t.TemplateID, &t.Name, &t.BaseImage, &t.WarmupScript, &t.Tier, &t.PinInL1Tmpfs,
		&t.TraceHotPages, &t.Phase, &t.GoldenSnapshotID, &t.StatusMessage, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &t, err
}

func (s *PostgresMetadataStore) UpdateTemplateStatus(ctx context.Context, templateID, phase, goldenSnapshotID, msg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE kt_templates
		SET phase = $2, golden_snapshot_id = COALESCE(NULLIF($3, ''), golden_snapshot_id),
		    status_message = $4, updated_at = $5
		WHERE template_id = $1`,
		templateID, phase, goldenSnapshotID, msg, time.Now().UTC(),
	)
	return err
}

func (s *PostgresMetadataStore) ListTemplates(ctx context.Context) ([]TemplateRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_id, name, base_image, warmup_script, tier, pin_in_l1_tmpfs,
		       trace_hot_pages, phase, golden_snapshot_id, status_message, created_at, updated_at
		FROM kt_templates ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TemplateRecord
	for rows.Next() {
		var t TemplateRecord
		if err := rows.Scan(
			&t.TemplateID, &t.Name, &t.BaseImage, &t.WarmupScript, &t.Tier, &t.PinInL1Tmpfs,
			&t.TraceHotPages, &t.Phase, &t.GoldenSnapshotID, &t.StatusMessage, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *PostgresMetadataStore) LockSnapshotSet(ctx context.Context, exclusive bool) (func(), error) {
	tx, err := s.snapshotLocks.Begin(ctx)
	if err != nil {
		return nil, err
	}
	release := func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(clean)
	}
	query := "SELECT pg_advisory_xact_lock_shared($1)"
	if exclusive {
		query = "SELECT pg_advisory_xact_lock($1)"
	}
	if _, err := tx.Exec(ctx, query, advisoryKey("snapshot-publications")); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (s *PostgresMetadataStore) CreateSnapshot(ctx context.Context, snap *SnapshotRecord) error {
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	fpRaw, _ := json.Marshal(snap.Fingerprint)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO kt_snapshots (
			snapshot_id, kind, template_id, source_sandbox_id, parent_snapshot_id,
			base_image, manifest_uri, origin_node_id, origin_peer_addr,
			total_memory_bytes, dirty_memory_bytes, dirty_chunks, reused_chunks,
			superseded, bootstrap_token, fingerprint_json, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		snap.SnapshotID, string(snap.Kind), snap.TemplateID, snap.SourceSandboxID, snap.ParentSnapshotID,
		snap.BaseImage, snap.ManifestURI, snap.OriginNodeID, snap.OriginPeerAddr,
		snap.TotalMemoryBytes, snap.DirtyMemoryBytes, snap.DirtyChunks, snap.ReusedChunks,
		snap.Superseded, snap.BootstrapToken, fpRaw, snap.CreatedAt,
	)
	return err
}

func (s *PostgresMetadataStore) GetSnapshot(ctx context.Context, snapshotID string) (*SnapshotRecord, error) {
	var snap SnapshotRecord
	var kindStr string
	var fpRaw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT snapshot_id, kind, template_id, source_sandbox_id, parent_snapshot_id,
		       base_image, manifest_uri, origin_node_id, origin_peer_addr,
		       total_memory_bytes, dirty_memory_bytes, dirty_chunks, reused_chunks,
		       superseded, bootstrap_token, fingerprint_json, created_at
		FROM kt_snapshots WHERE snapshot_id = $1`, snapshotID,
	).Scan(
		&snap.SnapshotID, &kindStr, &snap.TemplateID, &snap.SourceSandboxID, &snap.ParentSnapshotID,
		&snap.BaseImage, &snap.ManifestURI, &snap.OriginNodeID, &snap.OriginPeerAddr,
		&snap.TotalMemoryBytes, &snap.DirtyMemoryBytes, &snap.DirtyChunks, &snap.ReusedChunks,
		&snap.Superseded, &snap.BootstrapToken, &fpRaw, &snap.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	snap.Kind = toSnapshotKind(kindStr)
	_ = json.Unmarshal(fpRaw, &snap.Fingerprint)
	return &snap, nil
}

func (s *PostgresMetadataStore) MarkSnapshotSuperseded(ctx context.Context, snapshotID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE kt_snapshots SET superseded = TRUE WHERE snapshot_id = $1`, snapshotID)
	return err
}

func (s *PostgresMetadataStore) ListGCandidateSnapshots(ctx context.Context) ([]SnapshotRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT s.snapshot_id, s.kind, s.template_id, s.source_sandbox_id, s.parent_snapshot_id,
		       s.base_image, s.manifest_uri, s.origin_node_id, s.origin_peer_addr,
		       s.total_memory_bytes, s.dirty_memory_bytes, s.dirty_chunks, s.reused_chunks,
		       s.superseded, s.bootstrap_token, s.fingerprint_json, s.created_at
		FROM kt_snapshots s
		WHERE s.superseded = TRUE
		  AND NOT EXISTS (SELECT 1 FROM kt_sandboxes b WHERE b.active_snapshot_id = s.snapshot_id)
		  AND NOT EXISTS (SELECT 1 FROM kt_snapshots c WHERE c.parent_snapshot_id = s.snapshot_id)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotRecord
	for rows.Next() {
		var snap SnapshotRecord
		var kindStr string
		var fpRaw []byte
		if err := rows.Scan(
			&snap.SnapshotID, &kindStr, &snap.TemplateID, &snap.SourceSandboxID, &snap.ParentSnapshotID,
			&snap.BaseImage, &snap.ManifestURI, &snap.OriginNodeID, &snap.OriginPeerAddr,
			&snap.TotalMemoryBytes, &snap.DirtyMemoryBytes, &snap.DirtyChunks, &snap.ReusedChunks,
			&snap.Superseded, &snap.BootstrapToken, &fpRaw, &snap.CreatedAt,
		); err != nil {
			return nil, err
		}
		snap.Kind = toSnapshotKind(kindStr)
		_ = json.Unmarshal(fpRaw, &snap.Fingerprint)
		out = append(out, snap)
	}
	return out, rows.Err()
}

func (s *PostgresMetadataStore) ListAllSnapshots(ctx context.Context) ([]SnapshotRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT snapshot_id, kind, template_id, source_sandbox_id, parent_snapshot_id,
		       base_image, manifest_uri, origin_node_id, origin_peer_addr,
		       total_memory_bytes, dirty_memory_bytes, dirty_chunks, reused_chunks,
		       superseded, bootstrap_token, fingerprint_json, created_at
		FROM kt_snapshots`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotRecord
	for rows.Next() {
		var snap SnapshotRecord
		var kindStr string
		var fpRaw []byte
		if err := rows.Scan(
			&snap.SnapshotID, &kindStr, &snap.TemplateID, &snap.SourceSandboxID, &snap.ParentSnapshotID,
			&snap.BaseImage, &snap.ManifestURI, &snap.OriginNodeID, &snap.OriginPeerAddr,
			&snap.TotalMemoryBytes, &snap.DirtyMemoryBytes, &snap.DirtyChunks, &snap.ReusedChunks,
			&snap.Superseded, &snap.BootstrapToken, &fpRaw, &snap.CreatedAt,
		); err != nil {
			return nil, err
		}
		snap.Kind = toSnapshotKind(kindStr)
		_ = json.Unmarshal(fpRaw, &snap.Fingerprint)
		out = append(out, snap)
	}
	return out, rows.Err()
}

func (s *PostgresMetadataStore) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM kt_snapshots WHERE snapshot_id = $1`, snapshotID)
	return err
}

func (s *PostgresMetadataStore) CreateSandbox(ctx context.Context, sbx *SandboxRecord) error {
	now := time.Now().UTC()
	if sbx.CreatedAt.IsZero() {
		sbx.CreatedAt = now
	}
	sbx.UpdatedAt = now
	raw, err := json.Marshal(sbx)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO kt_sandboxes (sandbox_id, payload_json, state, active_snapshot_id, updated_at)
		VALUES ($1, $2, $3, $4, $5)`,
		sbx.SandboxID, raw, string(sbx.State), sbx.ActiveSnapshotID, sbx.UpdatedAt,
	)
	return err
}

func (s *PostgresMetadataStore) GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT payload_json FROM kt_sandboxes WHERE sandbox_id = $1`, sandboxID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var sbx SandboxRecord
	if err := json.Unmarshal(raw, &sbx); err != nil {
		return nil, err
	}
	return &sbx, nil
}

func (s *PostgresMetadataStore) SaveSandbox(ctx context.Context, sbx *SandboxRecord) error {
	sbx.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(sbx)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE kt_sandboxes
		SET payload_json = $2, state = $3, active_snapshot_id = $4, updated_at = $5
		WHERE sandbox_id = $1`,
		sbx.SandboxID, raw, string(sbx.State), sbx.ActiveSnapshotID, sbx.UpdatedAt,
	)
	return err
}

func (s *PostgresMetadataStore) ListSandboxesToSweep(ctx context.Context) ([]SandboxRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT payload_json FROM kt_sandboxes WHERE state IN ($1,$2,$4,$5) OR (state=$3 AND coalesce(payload_json->>'currentSlotId','') <> '')`, string(SandboxStateRunning), string(SandboxStateSuspending), string(SandboxStateSuspended), string(SandboxStateCreating), string(SandboxStateResuming))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SandboxRecord
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var sbx SandboxRecord
		if err := json.Unmarshal(raw, &sbx); err == nil {
			out = append(out, sbx)
		}
	}
	return out, rows.Err()
}

func (s *PostgresMetadataStore) DeleteSandbox(ctx context.Context, sandboxID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM kt_sandboxes WHERE sandbox_id = $1`, sandboxID)
	return err
}
