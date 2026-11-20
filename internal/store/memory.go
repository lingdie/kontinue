package store

import (
	"context"
	"sync"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func toSnapshotKind(s string) protocol.SnapshotKind {
	return protocol.SnapshotKind(s)
}

// MemoryMetadataStore provides an in-memory transactional MetadataStore with 100% behavioral parity
// to PostgresMetadataStore for fast unit tests, e2e tests, and standalone dev clusters.
type MemoryMetadataStore struct {
	mu        sync.RWMutex
	templates map[string]TemplateRecord
	snapshots map[string]SnapshotRecord
	sandboxes map[string]SandboxRecord
	leaders   map[string]bool
}

func NewMemoryMetadataStore() *MemoryMetadataStore {
	return &MemoryMetadataStore{
		templates: make(map[string]TemplateRecord),
		snapshots: make(map[string]SnapshotRecord),
		sandboxes: make(map[string]SandboxRecord),
		leaders:   make(map[string]bool),
	}
}

func (m *MemoryMetadataStore) TryLeader(_ context.Context, name string) (func(), bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leaders[name] {
		return func() {}, false, nil
	}
	m.leaders[name] = true
	return func() {
		m.mu.Lock()
		delete(m.leaders, name)
		m.mu.Unlock()
	}, true, nil
}

func (m *MemoryMetadataStore) Migrate(_ context.Context) error { return nil }

func (m *MemoryMetadataStore) CreateTemplate(_ context.Context, tpl *TemplateRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	if tpl.CreatedAt.IsZero() {
		tpl.CreatedAt = now
	}
	tpl.UpdatedAt = now
	m.templates[tpl.TemplateID] = *tpl
	return nil
}

func (m *MemoryMetadataStore) GetTemplate(_ context.Context, templateID string) (*TemplateRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.templates[templateID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := t
	return &cp, nil
}

func (m *MemoryMetadataStore) UpdateTemplateStatus(_ context.Context, templateID, phase, goldenSnapshotID, msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.templates[templateID]
	if !ok {
		return ErrNotFound
	}
	t.Phase = phase
	if goldenSnapshotID != "" {
		t.GoldenSnapshotID = goldenSnapshotID
	}
	t.StatusMessage = msg
	t.UpdatedAt = time.Now().UTC()
	m.templates[templateID] = t
	return nil
}

func (m *MemoryMetadataStore) ListTemplates(_ context.Context) ([]TemplateRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TemplateRecord, 0, len(m.templates))
	for _, v := range m.templates {
		out = append(out, v)
	}
	return out, nil
}

func (m *MemoryMetadataStore) CreateSnapshot(_ context.Context, snap *SnapshotRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	m.snapshots[snap.SnapshotID] = *snap
	return nil
}

func (m *MemoryMetadataStore) GetSnapshot(_ context.Context, snapshotID string) (*SnapshotRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.snapshots[snapshotID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := s
	return &cp, nil
}

func (m *MemoryMetadataStore) MarkSnapshotSuperseded(_ context.Context, snapshotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.snapshots[snapshotID]; ok {
		s.Superseded = true
		m.snapshots[snapshotID] = s
	}
	return nil
}

func (m *MemoryMetadataStore) ListGCandidateSnapshots(_ context.Context) ([]SnapshotRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	referencedBySandbox := make(map[string]bool)
	for _, b := range m.sandboxes {
		if b.ActiveSnapshotID != "" {
			referencedBySandbox[b.ActiveSnapshotID] = true
		}
	}
	referencedAsParent := make(map[string]bool)
	for _, s := range m.snapshots {
		if s.ParentSnapshotID != "" {
			referencedAsParent[s.ParentSnapshotID] = true
		}
	}

	var out []SnapshotRecord
	for _, s := range m.snapshots {
		if s.Superseded && !referencedBySandbox[s.SnapshotID] && !referencedAsParent[s.SnapshotID] {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *MemoryMetadataStore) ListAllSnapshots(_ context.Context) ([]SnapshotRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SnapshotRecord, 0, len(m.snapshots))
	for _, s := range m.snapshots {
		out = append(out, s)
	}
	return out, nil
}

func (m *MemoryMetadataStore) DeleteSnapshot(_ context.Context, snapshotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.snapshots, snapshotID)
	return nil
}

func (m *MemoryMetadataStore) CreateSandbox(_ context.Context, sbx *SandboxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	if sbx.CreatedAt.IsZero() {
		sbx.CreatedAt = now
	}
	sbx.UpdatedAt = now
	m.sandboxes[sbx.SandboxID] = cloneSandbox(*sbx)
	return nil
}

func (m *MemoryMetadataStore) GetSandbox(_ context.Context, sandboxID string) (*SandboxRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sandboxes[sandboxID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := cloneSandbox(s)
	return &cp, nil
}

func (m *MemoryMetadataStore) SaveSandbox(_ context.Context, sbx *SandboxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sbx.UpdatedAt = time.Now().UTC()
	m.sandboxes[sbx.SandboxID] = cloneSandbox(*sbx)
	return nil
}

func (m *MemoryMetadataStore) ListSandboxesToSweep(_ context.Context) ([]SandboxRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []SandboxRecord
	for _, s := range m.sandboxes {
		if s.State == SandboxStateRunning || s.State == SandboxStateSuspending || (s.State == SandboxStateSuspended && s.CurrentSlotID != "") {
			out = append(out, cloneSandbox(s))
		}
	}
	return out, nil
}

func (m *MemoryMetadataStore) DeleteSandbox(_ context.Context, sandboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sandboxes, sandboxID)
	return nil
}

func cloneSandbox(in SandboxRecord) SandboxRecord {
	if in.EnvVars != nil {
		cp := make(map[string]string, len(in.EnvVars))
		for k, v := range in.EnvVars {
			cp[k] = v
		}
		in.EnvVars = cp
	}
	return in
}

// MemoryRuntimeCatalog implements RuntimeCatalog with identical atomic fencing semantics to Redis Lua scripts.
type MemoryRuntimeCatalog struct {
	mu          sync.Mutex
	idlePools   map[string][]protocol.CarrierSlot
	routes      map[string]protocol.RouteRecord
	activeAt    map[string]time.Time
	inflight    map[string]int64
	admissions  map[string]map[string]time.Time
	nodes       map[string]nodeEntry
	idempotency map[string]idempEntry
}

type nodeEntry struct {
	hb      protocol.NodeHeartbeat
	expires time.Time
}

type idempEntry struct {
	status  int
	body    []byte
	expires time.Time
}

func NewMemoryRuntimeCatalog() *MemoryRuntimeCatalog {
	return &MemoryRuntimeCatalog{
		idlePools:   make(map[string][]protocol.CarrierSlot),
		routes:      make(map[string]protocol.RouteRecord),
		activeAt:    make(map[string]time.Time),
		inflight:    make(map[string]int64),
		admissions:  make(map[string]map[string]time.Time),
		nodes:       make(map[string]nodeEntry),
		idempotency: make(map[string]idempEntry),
	}
}

func (c *MemoryRuntimeCatalog) RegisterIdleSlot(_ context.Context, slot protocol.CarrierSlot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot.Tier == "" {
		slot.Tier = protocol.DefaultCarrierTier
	}
	k := protocol.IdlePoolKey(slot.NodeID, slot.Tier)
	c.idlePools[k] = append(c.idlePools[k], slot)
	return nil
}

func (c *MemoryRuntimeCatalog) RemoveIdleSlot(_ context.Context, nodeID, tier, slotID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	k := protocol.IdlePoolKey(nodeID, tier)
	list := c.idlePools[k]
	filtered := list[:0]
	for _, s := range list {
		if s.SlotID != slotID {
			filtered = append(filtered, s)
		}
	}
	c.idlePools[k] = filtered
	return nil
}

func (c *MemoryRuntimeCatalog) ClaimIdleSlot(_ context.Context, nodeID, tier string) (*protocol.CarrierSlot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	k := protocol.IdlePoolKey(nodeID, tier)
	list := c.idlePools[k]
	if len(list) == 0 {
		return nil, ErrNoIdleCarrierSlot
	}
	slot := list[len(list)-1]
	c.idlePools[k] = list[:len(list)-1]
	return &slot, nil
}

func (c *MemoryRuntimeCatalog) CountIdleSlots(_ context.Context, nodeID, tier string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	return len(c.idlePools[protocol.IdlePoolKey(nodeID, tier)]), nil
}

func (c *MemoryRuntimeCatalog) PublishRoute(_ context.Context, route protocol.RouteRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.routes[route.SandboxID]; ok && cur.ExecutionID > route.ExecutionID {
		return ErrStaleExecutionFence
	}
	c.routes[route.SandboxID] = route
	return nil
}

func (c *MemoryRuntimeCatalog) GetRoute(_ context.Context, sandboxID string) (*protocol.RouteRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.routes[sandboxID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := r
	return &cp, nil
}

func (c *MemoryRuntimeCatalog) DeleteRouteFenced(_ context.Context, sandboxID string, executionID int64, slotID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.routes[sandboxID]; ok && (cur.ExecutionID != executionID || cur.SlotID != slotID) {
		return ErrStaleExecutionFence
	}
	delete(c.routes, sandboxID)
	return nil
}

func (c *MemoryRuntimeCatalog) AdmitRoute(_ context.Context, id, requestID string) (*protocol.RouteRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.routes[id]
	if !ok {
		return nil, ErrNotFound
	}
	if c.admissions[id] == nil {
		c.admissions[id] = make(map[string]time.Time)
	}
	c.admissions[id][requestID] = time.Now().Add(AdmissionTTL)
	return &r, nil
}

func (c *MemoryRuntimeCatalog) RenewAdmission(_ context.Context, id, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !time.Now().Before(c.admissions[id][requestID]) {
		return ErrNotFound
	}
	c.admissions[id][requestID] = time.Now().Add(AdmissionTTL)
	return nil
}
func (c *MemoryRuntimeCatalog) FinishAdmission(_ context.Context, id, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.admissions[id], requestID)
	if len(c.admissions[id]) == 0 {
		delete(c.admissions, id)
	}
	return nil
}
func (c *MemoryRuntimeCatalog) inflightLocked(id string) int64 {
	now := time.Now()
	for key, expires := range c.admissions[id] {
		if !now.Before(expires) {
			delete(c.admissions[id], key)
		}
	}
	return c.inflight[id] + int64(len(c.admissions[id]))
}

func (c *MemoryRuntimeCatalog) DrainRoute(_ context.Context, r protocol.RouteRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.routes[r.SandboxID]; ok && (cur.SlotID != r.SlotID || cur.ExecutionID != r.ExecutionID) {
		return ErrStaleExecutionFence
	}
	if c.inflightLocked(r.SandboxID) > 0 {
		return ErrTransitionLocked
	}
	delete(c.routes, r.SandboxID)
	return nil
}

func (c *MemoryRuntimeCatalog) TouchSandboxActive(_ context.Context, sandboxID string, at time.Time) error {
	c.mu.Lock()
	c.activeAt[sandboxID] = at
	c.mu.Unlock()
	return nil
}

func (c *MemoryRuntimeCatalog) AdjustInflight(_ context.Context, sandboxID string, delta int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.inflight[sandboxID] + delta
	if n <= 0 {
		delete(c.inflight, sandboxID)
		return 0, nil
	}
	c.inflight[sandboxID] = n
	return n, nil
}

func (c *MemoryRuntimeCatalog) Inflight(_ context.Context, sandboxID string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflightLocked(sandboxID), nil
}

func (c *MemoryRuntimeCatalog) GetLastActive(_ context.Context, sandboxID string) (time.Time, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.activeAt[sandboxID]
	return t, ok, nil
}

func (c *MemoryRuntimeCatalog) UpsertNodeHeartbeat(_ context.Context, hb protocol.NodeHeartbeat, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	hb.UpdatedAt = time.Now().UTC()
	c.nodes[hb.NodeID] = nodeEntry{hb: hb, expires: time.Now().Add(ttl)}
	return nil
}

func (c *MemoryRuntimeCatalog) ListHealthyNodes(_ context.Context) ([]protocol.NodeHeartbeat, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	var out []protocol.NodeHeartbeat
	for id, e := range c.nodes {
		if now.After(e.expires) {
			delete(c.nodes, id)
			continue
		}
		out = append(out, e.hb)
	}
	return out, nil
}

func (c *MemoryRuntimeCatalog) SaveIdempotency(_ context.Context, key string, statusCode int, body []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idempotency[key] = idempEntry{status: statusCode, body: append([]byte(nil), body...), expires: time.Now().Add(ttl)}
	return nil
}

func (c *MemoryRuntimeCatalog) GetIdempotency(_ context.Context, key string) (int, []byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.idempotency[key]
	if !ok || time.Now().After(e.expires) {
		delete(c.idempotency, key)
		return 0, nil, false, nil
	}
	return e.status, append([]byte(nil), e.body...), true, nil
}
