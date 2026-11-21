package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
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

ALTER TABLE kt_templates ADD COLUMN IF NOT EXISTS build JSONB;
ALTER TABLE kt_templates ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'local';

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

CREATE TABLE IF NOT EXISTS kt_fork_receipts (
    source_id TEXT NOT NULL REFERENCES kt_sandboxes(sandbox_id) ON DELETE CASCADE,
    request_key TEXT NOT NULL,
    receipt JSONB NOT NULL,
    PRIMARY KEY (source_id, request_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_kt_sandboxes_create_key
ON kt_sandboxes ((payload_json->>'createKey')) WHERE payload_json ? 'createKey';
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
	acquireCtx, span := telemetry.StartSpan(ctx, "kontinue-apiserver", "postgres.acquire_advisory_connection")
	conn, err := s.locks.Acquire(acquireCtx)
	telemetry.EndSpan(span, err)
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
			trace_hot_pages, phase, golden_snapshot_id, status_message, created_at, updated_at, build, tenant_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		tpl.TemplateID, tpl.Name, tpl.BaseImage, tpl.WarmupScript, tpl.Tier, tpl.PinInL1Tmpfs,
		tpl.TraceHotPages, tpl.Phase, tpl.GoldenSnapshotID, tpl.StatusMessage, tpl.CreatedAt, tpl.UpdatedAt, tpl.Build, tpl.TenantID,
	)
	return err
}

func (s *PostgresMetadataStore) GetTemplate(ctx context.Context, templateID string) (*TemplateRecord, error) {
	var t TemplateRecord
	err := s.pool.QueryRow(ctx, `
		SELECT template_id, name, base_image, warmup_script, tier, pin_in_l1_tmpfs,
		       trace_hot_pages, phase, golden_snapshot_id, status_message, created_at, updated_at, build, tenant_id
		FROM kt_templates WHERE template_id = $1`, templateID,
	).Scan(
		&t.TemplateID, &t.Name, &t.BaseImage, &t.WarmupScript, &t.Tier, &t.PinInL1Tmpfs,
		&t.TraceHotPages, &t.Phase, &t.GoldenSnapshotID, &t.StatusMessage, &t.CreatedAt, &t.UpdatedAt, &t.Build, &t.TenantID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &t, err
}

func (s *PostgresMetadataStore) SaveTemplateBuild(ctx context.Context, t *TemplateRecord) error {
	t.UpdatedAt = time.Now().UTC()
	tag, err := s.pool.Exec(ctx, `UPDATE kt_templates SET phase=$2,golden_snapshot_id=$3,status_message=$4,build=$5,updated_at=$6 WHERE template_id=$1`, t.TemplateID, t.Phase, t.GoldenSnapshotID, t.StatusMessage, t.Build, t.UpdatedAt)
	if err == nil && tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresMetadataStore) ListPendingTemplateIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT template_id FROM kt_templates WHERE build IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *PostgresMetadataStore) ListTemplates(ctx context.Context) ([]TemplateRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_id, name, base_image, warmup_script, tier, pin_in_l1_tmpfs,
		       trace_hot_pages, phase, golden_snapshot_id, status_message, created_at, updated_at, tenant_id
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
			&t.TraceHotPages, &t.Phase, &t.GoldenSnapshotID, &t.StatusMessage, &t.CreatedAt, &t.UpdatedAt, &t.TenantID,
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
		  AND NOT EXISTS (SELECT 1 FROM kt_snapshots c WHERE c.parent_snapshot_id = s.snapshot_id)
		  AND NOT EXISTS (SELECT 1 FROM kt_templates t WHERE t.golden_snapshot_id = s.snapshot_id)`)
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

// API process loss releases its advisory lock while the worker can still be
// uploading. The sandbox intent identifies the capture across that gap.
func (s *PostgresMetadataStore) UnpublishedSnapshot(ctx context.Context) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
        SELECT p.id FROM kt_sandboxes AS b
        CROSS JOIN LATERAL (VALUES (b.payload_json->>'pendingSnapshotId'),
                                   (b.payload_json->'pendingFork'->>'snapshotId')) AS p(id)
        WHERE p.id <> '' AND NOT EXISTS (SELECT 1 FROM kt_snapshots s WHERE s.snapshot_id=p.id)
        UNION ALL
        SELECT t.build->>'snapshotId' FROM kt_templates AS t
        WHERE t.phase=$1 AND t.build IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM kt_snapshots s WHERE s.snapshot_id=t.build->>'snapshotId')
        LIMIT 1`, TemplateCapturing).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
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
	return insertSandbox(ctx, s.pool, sbx)
}

type sandboxWriter interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func insertSandbox(ctx context.Context, db sandboxWriter, sbx *SandboxRecord) error {
	now := time.Now().UTC()
	if sbx.CreatedAt.IsZero() {
		sbx.CreatedAt = now
	}
	sbx.UpdatedAt = now
	raw, err := json.Marshal(sbx)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		INSERT INTO kt_sandboxes (sandbox_id, payload_json, state, active_snapshot_id, updated_at)
		VALUES ($1, $2, $3, $4, $5)`,
		sbx.SandboxID, raw, string(sbx.State), sbx.ActiveSnapshotID, sbx.UpdatedAt,
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrSandboxExists
	}
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
	return updateSandbox(ctx, s.pool, sbx)
}

func updateSandbox(ctx context.Context, db sandboxWriter, sbx *SandboxRecord) error {
	sbx.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(sbx)
	if err != nil {
		return err
	}
	tag, err := db.Exec(ctx, `
		UPDATE kt_sandboxes
		SET payload_json = $2, state = $3, active_snapshot_id = $4, updated_at = $5
		WHERE sandbox_id = $1`,
		sbx.SandboxID, raw, string(sbx.State), sbx.ActiveSnapshotID, sbx.UpdatedAt,
	)
	if err == nil && tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresMetadataStore) GetForkReceipt(ctx context.Context, sourceID, requestKey string) (*ForkReceipt, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT receipt FROM kt_fork_receipts WHERE source_id=$1 AND request_key=$2`, sourceID, requestKey).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var receipt ForkReceipt
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func (s *PostgresMetadataStore) CommitFork(ctx context.Context, source *SandboxRecord, children []*SandboxRecord) error {
	fork := source.PendingFork
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, child := range children {
			if err := insertSandbox(ctx, tx, child); err != nil {
				return err
			}
		}
		if fork.RequestKey != "" {
			raw, err := json.Marshal(fork.ForkReceipt)
			if err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO kt_fork_receipts VALUES ($1,$2,$3)`, source.SandboxID, fork.RequestKey, raw); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE kt_snapshots SET superseded=TRUE WHERE snapshot_id=$1 AND kind=$2`, fork.SnapshotID, string(protocol.SnapshotKindLiveFork)); err != nil {
			return err
		}
		updated := *source
		updated.PendingFork = nil
		return updateSandbox(ctx, tx, &updated)
	})
}

func (s *PostgresMetadataStore) ListSandboxesToSweep(ctx context.Context) ([]SandboxRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT payload_json FROM kt_sandboxes WHERE state IN ($1,$2,$4,$5,$6) OR (state=$3 AND (coalesce(payload_json->>'currentSlotId','') <> '' OR payload_json ? 'pendingFork')) OR (state=$7 AND (coalesce(payload_json->>'currentSlotId','') <> '' OR payload_json ? 'pendingPlacement'))`, string(SandboxStateRunning), string(SandboxStateSuspending), string(SandboxStateSuspended), string(SandboxStateCreating), string(SandboxStateResuming), string(SandboxStateDeleting), string(SandboxStateFailed))
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
