// Package manager owns the set of running recorders and persists their
// definitions so they survive process restarts.
package manager

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"seath/internal/auth"
	"seath/internal/config"
	"seath/internal/natspreset"
	"seath/internal/pg"
	"seath/internal/prototypes"
	"seath/internal/recorder"
	"seath/internal/schema"
)

// Errors returned by the manager, mapped to HTTP status codes by the API layer.
var (
	ErrNotFound  = errors.New("recording not found")
	ErrConflict  = errors.New("recording is running")
	ErrValidate  = errors.New("invalid recording")
	ErrDuplicate = errors.New("table name already in use")
	ErrInUse     = errors.New("topic is in use by a recording")
	ErrProto     = errors.New("invalid proto catalogue")

	// Mapping sentinels: ErrValidate/ErrDuplicate word themselves as recording
	// errors, which surfaces verbatim and nonsensically in the registry UI.
	ErrMapping       = errors.New("invalid topic mapping")
	ErrMappingExists = errors.New("topic already mapped")
)

// JobView combines a recorder's definition with its live status and the
// on-disk footprint of its table for the API.
type JobView struct {
	recorder.JobSpec
	recorder.Snapshot
	TableRows      int64  `json:"table_rows"`
	RowsEstimated  bool   `json:"rows_estimated"`
	TableSize      string `json:"table_size"`
	TableSizeBytes int64  `json:"table_size_bytes"`
	// SampleDesc renders the active sampling rule, e.g. "10%" or
	// "entity_id%100<10", so the UI doesn't reimplement the formatting.
	SampleDesc     string                   `json:"sample_desc"`
	SampleSummary  string                   `json:"sample_summary"`
	SamplingFields []recorder.SamplingField `json:"sampling_fields,omitempty"`
}

// Stats is database-wide information for the API.
type Stats struct {
	DatabaseSize      string `json:"database_size"`
	DatabaseSizeBytes int64  `json:"database_size_bytes"`
}

// Mapping is a database-owned topic→proto-type mapping. All mappings are equal:
// any can be edited or deleted.
type Mapping struct {
	Topic     string `json:"topic"`
	ProtoName string `json:"proto_name"`
}

// topicStore is the slice of Store the mapping mutations use. It is an
// interface so a test can hold a write open and observe that a second mutation
// is actually excluded, rather than assuming it.
type topicStore interface {
	AddCustomTopic(topic, protoName string) error
	DeleteCustomTopic(topic string) error
}

// Manager owns all recorders and their persistence.
type Manager struct {
	mu        sync.RWMutex
	recorders map[string]*recorder.Recorder
	// custom holds runtime topic→proto-name mappings. Guarded by mu.
	custom map[string]string
	// catalogue is an immutable database-backed runtime proto registry. Guarded
	// by mu and atomically replaced after a complete candidate validates.
	catalogue *prototypes.Registry

	// taps holds ephemeral diagnostic subscriptions, guarded by tapMu.
	tapMu sync.Mutex
	taps  map[string]*Tap

	store   *Store
	topics  topicStore
	db      *sql.DB
	dsn     string
	rootCtx context.Context

	// maxRetentionHours is the configured ceiling on a recording's retention
	// window. Retention is mandatory: the valid range is 1..maxRetentionHours.
	maxRetentionHours int
	queryTimeout      time.Duration
	queryMaxRows      int

	// presets is the live set of DB-configured NATS environments.
	presetMu sync.RWMutex
	presets  map[string]natspreset.Preset
}

// New builds a Manager. Call Reconcile after construction to load and resume
// persisted jobs. db is a shared pool used only for metadata queries (table
// stats, drops); each recorder opens its own pool for writes.
func New(ctx context.Context, db *sql.DB, dsn string, store *Store, maxRetentionHours, queryTimeoutSeconds, queryMaxRows int) *Manager {
	if maxRetentionHours < 1 {
		maxRetentionHours = config.DefaultMaxRetentionHours
	}
	if queryTimeoutSeconds < 1 {
		queryTimeoutSeconds = config.DefaultQueryTimeoutSeconds
	}
	if queryMaxRows < 1 {
		queryMaxRows = config.DefaultQueryMaxRows
	}
	return &Manager{
		maxRetentionHours: maxRetentionHours,
		queryTimeout:      time.Duration(queryTimeoutSeconds) * time.Second,
		queryMaxRows:      queryMaxRows,
		recorders:         make(map[string]*recorder.Recorder),
		custom:            make(map[string]string),
		catalogue:         prototypes.Empty(),
		taps:              make(map[string]*Tap),
		store:             store,
		topics:            mappingStore(store),
		db:                db,
		dsn:               dsn,
		presets:           make(map[string]natspreset.Preset),
		rootCtx:           ctx,
	}
}

// mappingStore avoids a typed-nil interface, so a Manager built without a store
// (tests) fails loudly at the write instead of silently succeeding.
func mappingStore(s *Store) topicStore {
	if s == nil {
		return nil
	}
	return s
}

// loadPresets rebuilds the live preset map exclusively from the database.
func (m *Manager) loadPresets() error {
	stored, err := m.store.LoadPresets()
	if err != nil {
		return err
	}
	loaded := make(map[string]natspreset.Preset, len(stored))
	for _, p := range stored {
		loaded[p.Name] = p
	}
	m.presetMu.Lock()
	m.presets = loaded
	m.presetMu.Unlock()
	return nil
}

func (m *Manager) resolvePreset(name string) (natspreset.Preset, bool) {
	m.presetMu.RLock()
	defer m.presetMu.RUnlock()
	p, ok := m.presets[name]
	return p, ok
}

// MaxRetentionHours is the configured ceiling on a retention window.
func (m *Manager) MaxRetentionHours() int { return m.maxRetentionHours }

// AddPreset adds or updates a DB-backed NATS environment.
func (m *Manager) AddPreset(p natspreset.Preset) error {
	if p.Name == "" || p.URL == "" {
		return fmt.Errorf("%w: name and url are required", ErrValidate)
	}
	if err := m.store.AddPreset(p); err != nil {
		return err
	}
	return m.loadPresets()
}

// CreatePreset adds an environment without replacing existing configuration.
func (m *Manager) CreatePreset(p natspreset.Preset) error {
	if p.Name == "" || p.URL == "" {
		return fmt.Errorf("%w: name and url are required", ErrValidate)
	}
	if err := m.store.CreatePreset(p); err != nil {
		return err
	}
	return m.loadPresets()
}

// DeletePreset removes an environment unless a recording still references it.
func (m *Manager) DeletePreset(name string) error {
	m.mu.RLock()
	for _, rec := range m.recorders {
		if rec.Spec().NATSEnv == name {
			m.mu.RUnlock()
			return fmt.Errorf("%w: %q is used by a recording", ErrInUse, name)
		}
	}
	m.mu.RUnlock()
	if err := m.store.DeletePreset(name); err != nil {
		return err
	}
	return m.loadPresets()
}

// resolveTopic maps a topic name to a fresh proto template from the stored
// mappings.
func (m *Manager) resolveTopic(t string) (proto.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.resolveTopicLocked(t)
}

// resolveTopicLocked is resolveTopic for callers already holding m.mu. The
// stored mappings are the only source.
func (m *Manager) resolveTopicLocked(t string) (proto.Message, error) {
	name, ok := m.custom[t]
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", t)
	}
	return m.catalogue.New(name)
}

// ResolveTopic returns a fresh proto template for a topic (registry or custom).
func (m *Manager) ResolveTopic(t string) (proto.Message, error) {
	return m.resolveTopic(t)
}

// Topics returns all selectable topic names, sorted.
func (m *Manager) Topics() []string {
	m.mu.RLock()
	out := make([]string, 0, len(m.custom))
	for t := range m.custom {
		out = append(out, t)
	}
	m.mu.RUnlock()
	sort.Strings(out)
	return out
}

// TopicMappings returns every stored mapping, sorted by topic.
func (m *Manager) TopicMappings() []Mapping {
	m.mu.RLock()
	out := make([]Mapping, 0, len(m.custom))
	for t, n := range m.custom {
		out = append(out, Mapping{Topic: t, ProtoName: n})
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// ProtoCatalogue is the registry page's complete control-plane view. Active
// modules define decoding; History contains staged and previous versions.
type ProtoCatalogue struct {
	Modules          []prototypes.Module `json:"modules"`
	History          []prototypes.Module `json:"history"`
	TypeCount        int                 `json:"type_count"`
	CompiledFallback bool                `json:"compiled_fallback"`
}

func (m *Manager) ProtoCatalogue() (ProtoCatalogue, error) {
	m.mu.RLock()
	modules := m.catalogue.Modules()
	typeCount := len(m.catalogue.List())
	m.mu.RUnlock()
	history, err := m.store.ListProtoModules()
	if err != nil {
		return ProtoCatalogue{}, err
	}
	for i := range history {
		history[i].DescriptorSet = nil
	}
	return ProtoCatalogue{Modules: modules, History: history, TypeCount: typeCount, CompiledFallback: false}, nil
}

func (m *Manager) ProtoTypes() []prototypes.TypeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.catalogue.List()
}

func (m *Manager) NewProto(fullName string) (proto.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.catalogue.New(fullName)
}

// ImportProtoModule validates and stores a staged descriptor set. Importing
// never changes live decoding and is therefore safe while recordings run.
func (m *Manager) ImportProtoModule(path, version string, descriptors []byte, actor string) (prototypes.Module, error) {
	path, version = strings.TrimSpace(path), strings.TrimSpace(version)
	if path == "" || version == "" {
		return prototypes.Module{}, fmt.Errorf("%w: module path and version are required", ErrProto)
	}
	if len(path) > 300 || len(version) > 100 {
		return prototypes.Module{}, fmt.Errorf("%w: module path or version is too long", ErrProto)
	}
	module := prototypes.Module{
		Path: path, Version: version, SHA256: prototypes.Digest(descriptors),
		DescriptorSet: descriptors, CreatedBy: actor,
	}
	active, err := m.store.LoadActiveProtoModules()
	if err != nil {
		return prototypes.Module{}, err
	}
	if _, err := prototypes.Build(replaceProtoModule(active, module)); err != nil {
		return prototypes.Module{}, fmt.Errorf("%w: %v", ErrProto, err)
	}
	stored, err := m.store.SaveProtoModule(module)
	if err != nil {
		return prototypes.Module{}, err
	}
	stored.DescriptorSet = nil
	return stored, nil
}

// ActivateProtoModule atomically replaces one module's active version. Existing
// recordings keep decoding with their pinned immutable module snapshots;
// activation only changes the catalogue used by future recordings.
// InvalidMappings are reported but retained for the admin to repair.
func (m *Manager) ActivateProtoModule(id int64) (module prototypes.Module, invalidMappings []string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := activationSnapshotError(m.recorders); err != nil {
		return prototypes.Module{}, nil, err
	}
	module, err = m.store.LoadProtoModule(id)
	if errors.Is(err, sql.ErrNoRows) {
		return prototypes.Module{}, nil, ErrNotFound
	}
	if err != nil {
		return prototypes.Module{}, nil, err
	}
	active, err := m.store.LoadActiveProtoModules()
	if err != nil {
		return prototypes.Module{}, nil, err
	}
	candidateModules := replaceProtoModule(active, module)
	candidate, err := prototypes.Build(candidateModules)
	if err != nil {
		return prototypes.Module{}, nil, fmt.Errorf("%w: %v", ErrProto, err)
	}
	for topicName, protoName := range m.custom {
		if !candidate.Exists(protoName) {
			invalidMappings = append(invalidMappings, topicName)
		}
	}
	sort.Strings(invalidMappings)
	if err := m.store.ActivateProtoModule(id); err != nil {
		return prototypes.Module{}, nil, err
	}
	module.Active = true
	module.DescriptorSet = nil
	m.catalogue = candidate
	return module, invalidMappings, nil
}

func activationSnapshotError(recorders map[string]*recorder.Recorder) error {
	for recordingID, rec := range recorders {
		if !hasProtoSnapshot(rec.Spec()) {
			return fmt.Errorf("%w: recording %q has not captured its proto snapshot; restart Seath to migrate it before activation", ErrInUse, recordingID)
		}
	}
	return nil
}

func hasProtoSnapshot(spec recorder.JobSpec) bool {
	if spec.ProtoName == "" || len(spec.ProtoModuleIDs) == 0 {
		return false
	}
	for _, id := range spec.ProtoModuleIDs {
		if id < 1 {
			return false
		}
	}
	return true
}

// currentProtoSnapshotLocked resolves a topic from the active catalogue and
// captures the exact immutable module IDs needed to reconstruct that catalogue
// after future activations. The caller must hold m.mu.
func (m *Manager) currentProtoSnapshotLocked(topic string) (proto.Message, string, []int64, error) {
	protoName, ok := m.custom[topic]
	if !ok {
		return nil, "", nil, fmt.Errorf("unknown topic %q", topic)
	}
	tmpl, err := m.catalogue.New(protoName)
	if err != nil {
		return nil, "", nil, err
	}
	modules := m.catalogue.Modules()
	ids := make([]int64, 0, len(modules))
	for _, module := range modules {
		if module.ID < 1 {
			return nil, "", nil, fmt.Errorf("active proto module %s@%s has no persistent ID", module.Path, module.Version)
		}
		ids = append(ids, module.ID)
	}
	if len(ids) == 0 {
		return nil, "", nil, fmt.Errorf("active proto catalogue has no modules")
	}
	return tmpl, protoName, ids, nil
}

// fixedProtoResolver returns fresh messages of one pinned type. It deliberately
// ignores later topic mapping and active-catalogue changes.
func fixedProtoResolver(tmpl proto.Message) recorder.Resolver {
	messageType := tmpl.ProtoReflect().Type()
	return func(string) (proto.Message, error) {
		return messageType.New().Interface(), nil
	}
}

func replaceProtoModule(active []prototypes.Module, next prototypes.Module) []prototypes.Module {
	next.Active = true
	out := make([]prototypes.Module, 0, len(active)+1)
	for _, module := range active {
		if module.Path != next.Path {
			out = append(out, module)
		}
	}
	return append(out, next)
}

// AddTopicMapping registers a mapping, refusing to replace an existing one
// (use SetTopicMapping).
func (m *Manager) AddTopicMapping(t, protoName string) (Mapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setMappingLocked(t, protoName, false)
}

// topicPattern is stricter than NATS, which also accepts Unicode and most
// punctuation. Excluded here: whitespace and empty tokens (NATS rejects those
// too), the wildcards `*` and `>` (a recording decodes one concrete subject),
// and the characters that carry meaning in HTML, since a topic is rendered into
// the console's markup. Everything else NATS allows is permitted.
const topicExcluded = `\s.*><"'&` + "`" + `\x00-\x1f`

var topicPattern = regexp.MustCompile(`^[^` + topicExcluded + `]+(\.[^` + topicExcluded + `]+)*$`)

// classifyMapping validates a proposed mapping and returns what it would
// produce, persisting nothing. The caller must hold m.mu.
func (m *Manager) classifyMapping(t, protoName string) (Mapping, error) {
	if t == "" || protoName == "" {
		return Mapping{}, fmt.Errorf("%w: topic and proto type are both required", ErrMapping)
	}
	if !topicPattern.MatchString(t) {
		return Mapping{}, fmt.Errorf("%w: topic %q is not a plain dotted NATS subject", ErrMapping, t)
	}
	if !m.catalogue.Exists(protoName) {
		return Mapping{}, fmt.Errorf("%w: proto type %q is not in the active catalogue", ErrMapping, protoName)
	}
	return Mapping{Topic: t, ProtoName: protoName}, nil
}

// SetTopicMapping points a topic at a proto type, creating or replacing the
// mapping. Existing recordings are unaffected because each owns a pinned proto
// snapshot; only recordings created after this change use the new mapping.
func (m *Manager) SetTopicMapping(t, protoName string) (Mapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setMappingLocked(t, protoName, true)
}

// setMappingLocked keeps the guard, the row and the in-memory map one decision;
// racing callers would otherwise leave Postgres and the process disagreeing.
func (m *Manager) setMappingLocked(t, protoName string, replace bool) (Mapping, error) {
	if _, exists := m.custom[t]; exists && !replace {
		return Mapping{}, fmt.Errorf("%w: %q already has a mapping (edit it instead)", ErrMappingExists, t)
	}
	mp, err := m.classifyMapping(t, protoName)
	if err != nil {
		return Mapping{}, err
	}
	if err := m.topics.AddCustomTopic(t, protoName); err != nil {
		return Mapping{}, err
	}
	m.custom[t] = protoName
	return mp, nil
}

// DeleteTopicMapping removes a mapping, so no new recording can use the topic.
// Existing recordings continue with their pinned proto snapshots.
func (m *Manager) DeleteTopicMapping(t string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.custom[t]; !ok {
		return ErrNotFound
	}
	if err := m.topics.DeleteCustomTopic(t); err != nil {
		return err
	}
	delete(m.custom, t)
	return nil
}

// view builds a JobView, enriching the recorder's state with live table stats.
func (m *Manager) view(rec *recorder.Recorder) JobView {
	v := JobView{JobSpec: rec.Spec(), Snapshot: rec.Snapshot(), SampleDesc: rec.SampleDesc(), SampleSummary: rec.SampleSummary()}
	if tmpl, err := rec.NewMessage(); err == nil {
		v.SamplingFields = recorder.SamplingFields(tmpl)
	}
	rows, estimated, size, sizeBytes, err := pg.TableStats(m.db, rec.Spec().Table)
	if err == nil {
		v.TableRows, v.RowsEstimated, v.TableSize, v.TableSizeBytes = rows, estimated, size, sizeBytes
	} else {
		v.TableSize = "0 bytes"
	}
	return v
}

// Preview returns the most recent rows of a recording's table.
func (m *Manager) Preview(id string, limit int) ([]map[string]interface{}, error) {
	m.mu.RLock()
	rec, ok := m.recorders[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return pg.SampleRows(m.db, rec.Spec().Table, limit)
}

// Insights bundles a recording's definition with rich stats about its table.
type Insights struct {
	ID            string          `json:"id"`
	Topic         string          `json:"topic"`
	Table         string          `json:"table"`
	SampleRate    int             `json:"sample_rate"`
	NATSEnv       string          `json:"nats_env"`
	Status        string          `json:"status"`
	Exists        bool            `json:"exists"`
	Rows          int64           `json:"rows"`
	RowsEstimated bool            `json:"rows_estimated"`
	Size          string          `json:"size"`
	SizeBytes     int64           `json:"size_bytes"`
	Columns       []pg.ColumnInfo `json:"columns"`
	FirstAt       *time.Time      `json:"first_at"`
	LastAt        *time.Time      `json:"last_at"`
	CreatedBy     string          `json:"created_by"`
	CreatedAt     time.Time       `json:"created_at"`
}

// Insights returns table statistics for a recording.
func (m *Manager) Insights(id string) (Insights, error) {
	m.mu.RLock()
	rec, ok := m.recorders[id]
	m.mu.RUnlock()
	if !ok {
		return Insights{}, ErrNotFound
	}
	spec := rec.Spec()
	in := Insights{
		ID:         spec.ID,
		Topic:      spec.Topic,
		Table:      spec.Table,
		SampleRate: spec.SampleRate,
		NATSEnv:    spec.NATSEnv,
		Status:     rec.Snapshot().Status,
		CreatedBy:  spec.CreatedBy,
		CreatedAt:  spec.CreatedAt,
	}

	rows, estimated, size, sizeBytes, err := pg.TableStats(m.db, spec.Table)
	if err != nil {
		return Insights{}, err
	}
	in.Rows, in.RowsEstimated, in.Size, in.SizeBytes = rows, estimated, size, sizeBytes
	in.Exists = sizeBytes > 0 || rows > 0

	if cols, err := pg.TableColumns(m.db, spec.Table); err == nil {
		in.Columns = cols
		in.Exists = in.Exists || len(cols) > 0
	}
	if first, last, err := pg.TimeSpan(m.db, spec.Table); err == nil {
		if first.Valid {
			in.FirstAt = &first.Time
		}
		if last.Valid {
			in.LastAt = &last.Time
		}
	}
	return in, nil
}

// Rows returns a page of a recording's table rows.
func (m *Manager) Rows(id string, limit, offset int, order string) ([]string, []map[string]interface{}, error) {
	m.mu.RLock()
	rec, ok := m.recorders[id]
	m.mu.RUnlock()
	if !ok {
		return nil, nil, ErrNotFound
	}
	return pg.Rows(m.db, rec.Spec().Table, limit, offset, order)
}

// Query runs a read-only SQL query against the database with the configured row cap.
func (m *Manager) Query(ctx context.Context, sql string) ([]string, []map[string]interface{}, bool, error) {
	return pg.Query(ctx, m.db, sql, m.queryTimeout, m.queryMaxRows)
}

// ExportCSV streams every row of one recording's table, oldest first.
func (m *Manager) ExportCSV(ctx context.Context, id string, dst io.Writer) error {
	m.mu.RLock()
	rec, ok := m.recorders[id]
	m.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}
	return pg.StreamTableCSV(ctx, m.db, rec.Spec().Table, m.queryTimeout, dst)
}

// QueryTimeoutSeconds is the configured limit for SQL queries and exports.
func (m *Manager) QueryTimeoutSeconds() int {
	return int(m.queryTimeout / time.Second)
}

// QueryMaxRows is the configured result cap for interactive SQL queries.
func (m *Manager) QueryMaxRows() int {
	return m.queryMaxRows
}

// HostStats returns the database host's resource usage (system_stats extension).
// On error or when the extension is absent it returns {Available: false}.
func (m *Manager) HostStats() pg.PGHostStats {
	h, err := pg.HostStats(m.db)
	if err != nil {
		slog.Debug("host stats read failed", "err", err)
		return pg.PGHostStats{}
	}
	return h
}

// Stats returns database-wide statistics.
func (m *Manager) Stats() (Stats, error) {
	size, bytes, err := pg.DatabaseSize(m.db)
	if err != nil {
		return Stats{}, err
	}
	return Stats{DatabaseSize: size, DatabaseSizeBytes: bytes}, nil
}

// Reconcile loads persisted specs, registers them, and starts those whose
// desired state is running. A failed start is logged but never aborts boot.
func (m *Manager) Reconcile() error {
	if err := m.loadPresets(); err != nil {
		return err
	}
	activeModules, err := m.store.LoadActiveProtoModules()
	if err != nil {
		return fmt.Errorf("load active proto modules: %w", err)
	}
	if len(activeModules) > 0 {
		catalogue, err := prototypes.Build(activeModules)
		if err != nil {
			return fmt.Errorf("load active proto catalogue: %w", err)
		}
		m.mu.Lock()
		m.catalogue = catalogue
		m.mu.Unlock()
		slog.Info("loaded database-backed proto catalogue", "modules", len(activeModules), "types", len(catalogue.List()))
	} else {
		slog.Warn("no active database proto modules — import and activate a descriptor bundle before recording")
	}
	custom, err := m.store.LoadCustomTopics()
	if err != nil {
		return err
	}
	if len(custom) == 0 {
		slog.Warn("no topic mappings in the database — nothing is recordable until one is added")
	}
	// Rows predating topic validation are kept — a recording may depend on one —
	// but named, since they are the only way a topic outside topicPattern can
	// still reach the console.
	for t := range custom {
		if !topicPattern.MatchString(t) {
			slog.Warn("stored topic mapping predates subject validation", "topic", t)
		}
	}
	m.mu.Lock()
	m.custom = custom
	m.mu.Unlock()

	jobs, err := m.store.Load()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		var tmpl proto.Message
		if hasProtoSnapshot(j.Spec) {
			modules, err := m.store.LoadProtoModules(j.Spec.ProtoModuleIDs)
			if err != nil {
				return fmt.Errorf("load proto snapshot for recording %s: %w", j.Spec.ID, err)
			}
			registry, err := prototypes.Build(modules)
			if err != nil {
				return fmt.Errorf("build proto snapshot for recording %s: %w", j.Spec.ID, err)
			}
			tmpl, err = registry.New(j.Spec.ProtoName)
			if err != nil {
				return fmt.Errorf("resolve proto snapshot for recording %s: %w", j.Spec.ID, err)
			}
		} else {
			// Expand-phase migration for rows created before recording snapshots.
			// It is safe because activation cannot be served until Reconcile ends.
			m.mu.RLock()
			var protoName string
			var moduleIDs []int64
			tmpl, protoName, moduleIDs, err = m.currentProtoSnapshotLocked(j.Spec.Topic)
			m.mu.RUnlock()
			if err != nil {
				return fmt.Errorf("capture proto snapshot for recording %s: %w", j.Spec.ID, err)
			}
			if err := m.store.SetProtoSnapshot(j.Spec.ID, protoName, moduleIDs); err != nil {
				return fmt.Errorf("persist proto snapshot for recording %s: %w", j.Spec.ID, err)
			}
			j.Spec.ProtoName = protoName
			j.Spec.ProtoModuleIDs = moduleIDs
			slog.Info("captured legacy recording proto snapshot", "id", j.Spec.ID, "proto", protoName, "modules", len(moduleIDs))
		}
		rec := recorder.NewResolved(j.Spec, fixedProtoResolver(tmpl), tmpl)
		rec.SeedCounters(j.Received, j.Written, j.Errors)
		m.mu.Lock()
		m.recorders[j.Spec.ID] = rec
		m.mu.Unlock()
		if j.Spec.Desired == recorder.DesiredRunning {
			if err := m.startRecorder(rec); err != nil {
				slog.Warn("reconcile: failed to start recorder", "id", j.Spec.ID, "err", err)
			}
		}
	}
	go m.flushLoop()
	go m.retryLoop()
	go m.cleanupLoop()
	return nil
}

// cleanupLoop enforces per-recording retention by deleting rows older than the
// configured number of hours. Recordings with retention_hours == 0 keep data
// forever.
func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(config.RetentionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.rootCtx.Done():
			return
		case <-ticker.C:
			for _, j := range m.retentionJobs() {
				n, err := pg.DeleteOlderThan(m.db, j.table, j.hours)
				if err != nil {
					slog.Warn("retention cleanup failed", "table", j.table, "err", err)
				} else if n > 0 {
					slog.Info("retention cleanup", "table", j.table, "deleted", n, "older_than_hours", j.hours)
				}
			}
		}
	}
}

type retentionJob struct {
	table string
	hours int
}

// retentionJobs snapshots the recordings that have a retention window set.
func (m *Manager) retentionJobs() []retentionJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var jobs []retentionJob
	for _, rec := range m.recorders {
		s := rec.Spec()
		if s.RetentionHours > 0 {
			jobs = append(jobs, retentionJob{s.Table, s.RetentionHours})
		}
	}
	return jobs
}

// Ping checks database connectivity (used by the readiness endpoint).
func (m *Manager) Ping() error { return m.db.Ping() }

// DBStats returns the underlying connection-pool statistics (for the system
// monitor).
func (m *Manager) DBStats() sql.DBStats { return m.db.Stats() }

// RecordingCounts returns the total number of recordings and how many are
// currently running.
func (m *Manager) RecordingCounts() (total, running int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	total = len(m.recorders)
	for _, rec := range m.recorders {
		if rec.Snapshot().Status == recorder.StatusRunning {
			running++
		}
	}
	return total, running
}

// TapCount returns the number of active taps.
func (m *Manager) TapCount() int {
	m.tapMu.Lock()
	defer m.tapMu.Unlock()
	return len(m.taps)
}

// RecAggregate sums live recorder counters and connection/buffer health across
// all recordings (for the system monitor). It reads in-memory snapshots only —
// no database queries — so it is cheap to poll.
type RecAggregate struct {
	Received             uint64         `json:"received"`
	Written              uint64         `json:"written"`
	Errors               uint64         `json:"errors"`
	Dropped              uint64         `json:"dropped"`
	MaxBufferFillPercent float64        `json:"max_buffer_fill_percent"`
	NATS                 map[string]int `json:"nats"` // status -> count of running recorders
}

// RecorderAggregate returns summed counters and health across all recorders.
func (m *Manager) RecorderAggregate() RecAggregate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	agg := RecAggregate{NATS: map[string]int{}}
	for _, rec := range m.recorders {
		s := rec.Snapshot()
		agg.Received += s.Received
		agg.Written += s.Written
		agg.Errors += s.Errors
		agg.Dropped += s.Dropped
		if s.Status == recorder.StatusRunning {
			if s.BufferCap > 0 {
				if fill := float64(s.BufferLen) / float64(s.BufferCap) * 100; fill > agg.MaxBufferFillPercent {
					agg.MaxBufferFillPercent = fill
				}
			}
			if s.NATSStatus != "" {
				agg.NATS[s.NATSStatus]++
			}
		}
	}
	return agg
}

// ListAudit returns recent audit-log entries (admin-only at the API layer).
func (m *Manager) ListAudit(limit int) ([]auth.AuditEntry, error) {
	return auth.ListAudit(m.db, limit)
}

// RecordingAudit returns the audit trail of one recording, newest first.
func (m *Manager) RecordingAudit(id string, limit int) ([]auth.AuditEntry, error) {
	m.mu.RLock()
	_, ok := m.recorders[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return auth.ListRecordingAudit(m.db, id, limit)
}

// retryLoop periodically restarts recorders that should be running but are in
// an error state (e.g. NATS was unreachable at start), so they self-heal
// without manual intervention.
func (m *Manager) retryLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.rootCtx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			var retry []*recorder.Recorder
			for _, rec := range m.recorders {
				if rec.Spec().Desired == recorder.DesiredRunning && rec.Snapshot().Status == recorder.StatusError {
					retry = append(retry, rec)
				}
			}
			m.mu.RUnlock()
			for _, rec := range retry {
				slog.Info("auto-retrying errored recorder", "id", rec.Spec().ID)
				if err := m.startRecorder(rec); err != nil {
					slog.Warn("auto-retry failed", "id", rec.Spec().ID, "err", err)
				}
			}
		}
	}
}

// flushLoop periodically persists running recorders' counters so totals survive
// an unclean restart. Stops when the root context is cancelled.
func (m *Manager) flushLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.rootCtx.Done():
			return
		case <-ticker.C:
			m.flushAll()
		}
	}
}

func (m *Manager) flushAll() {
	m.mu.RLock()
	recs := make([]*recorder.Recorder, 0, len(m.recorders))
	for _, rec := range m.recorders {
		recs = append(recs, rec)
	}
	m.mu.RUnlock()
	for _, rec := range recs {
		m.flush(rec)
	}
}

func (m *Manager) flush(rec *recorder.Recorder) {
	snap := rec.Snapshot()
	if err := m.store.SaveCounters(rec.Spec().ID, snap.Received, snap.Written, snap.Errors); err != nil {
		slog.Warn("flush counters failed", "id", rec.Spec().ID, "err", err)
	}
}

// List returns all recordings with their live status.
func (m *Manager) List() []JobView {
	m.mu.RLock()
	defer m.mu.RUnlock()
	views := make([]JobView, 0, len(m.recorders))
	for _, rec := range m.recorders {
		views = append(views, m.view(rec))
	}
	return views
}

// Get returns one recording by id.
func (m *Manager) Get(id string) (JobView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.recorders[id]
	if !ok {
		return JobView{}, ErrNotFound
	}
	return m.view(rec), nil
}

// Create validates and persists a new (stopped) recording owned by actor.
func (m *Manager) Create(spec recorder.JobSpec, actor string) (JobView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tmpl, protoName, moduleIDs, err := m.currentProtoSnapshotLocked(spec.Topic)
	if err != nil {
		return JobView{}, fmt.Errorf("%w: %v", ErrValidate, err)
	}
	spec.ProtoName = protoName
	spec.ProtoModuleIDs = moduleIDs
	if err := m.validateWithTemplateLocked(spec, tmpl); err != nil {
		return JobView{}, err
	}
	if err := m.checkDuplicateTable(spec.Table, ""); err != nil {
		return JobView{}, err
	}

	spec.ID = newID()
	spec.Desired = recorder.DesiredStopped
	spec.CreatedBy = actor
	spec.CreatedAt = time.Now()
	if err := m.store.Save(spec); err != nil {
		return JobView{}, err
	}
	rec := recorder.NewResolved(spec, fixedProtoResolver(tmpl), tmpl)
	m.recorders[spec.ID] = rec
	return m.view(rec), nil
}

// Update edits a stopped recording's definition. It also returns the spec it
// replaced, captured under the same lock so a caller describing the change
// cannot observe a state some other request has already moved past.
func (m *Manager) Update(id string, spec recorder.JobSpec) (JobView, recorder.JobSpec, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recorders[id]
	if !ok {
		return JobView{}, recorder.JobSpec{}, ErrNotFound
	}
	if rec.Snapshot().Status == recorder.StatusRunning {
		return JobView{}, recorder.JobSpec{}, ErrConflict
	}
	prev := rec.Spec()
	var tmpl proto.Message
	var err error
	if spec.Topic == prev.Topic {
		// Ordinary edits retain the decoder snapshot that owns the existing table.
		tmpl, err = rec.NewMessage()
		spec.ProtoName = prev.ProtoName
		spec.ProtoModuleIDs = append([]int64(nil), prev.ProtoModuleIDs...)
	} else {
		// Moving to another topic intentionally adopts that topic's current schema.
		tmpl, spec.ProtoName, spec.ProtoModuleIDs, err = m.currentProtoSnapshotLocked(spec.Topic)
	}
	if err != nil {
		return JobView{}, recorder.JobSpec{}, fmt.Errorf("%w: %v", ErrValidate, err)
	}
	if err := m.validateWithTemplateLocked(spec, tmpl); err != nil {
		return JobView{}, recorder.JobSpec{}, err
	}
	if err := m.checkDuplicateTable(spec.Table, id); err != nil {
		return JobView{}, recorder.JobSpec{}, err
	}

	spec.ID = id
	spec.Desired = recorder.DesiredStopped
	// Ownership belongs to the original creator, not to whoever edits it.
	spec.CreatedBy = prev.CreatedBy
	spec.CreatedAt = prev.CreatedAt
	if err := m.store.Save(spec); err != nil {
		return JobView{}, recorder.JobSpec{}, err
	}
	rec = recorder.NewResolved(spec, fixedProtoResolver(tmpl), tmpl)
	m.recorders[id] = rec
	return m.view(rec), prev, nil
}

// SetSampling changes a recording's percentage and AND-composed field filters.
// It takes effect on the next message, even while the recording is running.
// The recorder rejects an unusable rule before swapping it in, so a bad request
// leaves the live recording alone and nothing is persisted.
func (m *Manager) SetSampling(id string, s recorder.Sampling) (JobView, error) {
	// Held across the persist so two concurrent changes cannot land in the
	// recorder and in Postgres in opposite orders (Create/Update do the same).
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recorders[id]
	if !ok {
		return JobView{}, ErrNotFound
	}
	tmpl, err := rec.NewMessage()
	if err != nil {
		return JobView{}, fmt.Errorf("%w: %v", ErrValidate, err)
	}
	if err := rec.ValidateSamplingChange(s, tmpl); err != nil {
		return JobView{}, fmt.Errorf("%w: %v", ErrValidate, err)
	}
	if err := m.store.SetSampling(id, s); err != nil {
		return JobView{}, err
	}
	if err := rec.SetSampling(s, tmpl); err != nil {
		// Validation above and this build are deterministic while m.mu excludes
		// concurrent changes. Roll persistence back if that invariant ever
		// changes, so restart behavior cannot disagree with the live gate.
		rollbackErr := m.store.SetSampling(id, rec.Spec().Sampling())
		if rollbackErr != nil {
			return JobView{}, fmt.Errorf("apply sampling: %v (rollback persistence: %v)", err, rollbackErr)
		}
		return JobView{}, fmt.Errorf("apply sampling: %w", err)
	}
	return m.view(rec), nil
}

// Delete removes a stopped recording and drops its Postgres table, returning
// the name of the table it dropped.
func (m *Manager) Delete(id string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recorders[id]
	if !ok {
		return "", ErrNotFound
	}
	if rec.Snapshot().Status == recorder.StatusRunning {
		return "", ErrConflict
	}
	table := rec.Spec().Table
	if err := m.store.Delete(id); err != nil {
		return "", err
	}
	if err := pg.DropTable(m.db, table); err != nil {
		return "", fmt.Errorf("drop table %q: %w", table, err)
	}
	delete(m.recorders, id)
	return table, nil
}

// Start begins a recording and marks its desired state running.
func (m *Manager) Start(id string) (JobView, error) {
	m.mu.Lock()
	rec, ok := m.recorders[id]
	m.mu.Unlock()
	if !ok {
		return JobView{}, ErrNotFound
	}
	// Starting an already-running recording is a no-op success (idempotent).
	if rec.Snapshot().Status == recorder.StatusRunning {
		return m.view(rec), nil
	}
	rec.SetDesired(recorder.DesiredRunning)
	if err := m.startRecorder(rec); err != nil {
		// Persist the intent even if the start failed, so reconcile retries.
		_ = m.store.SetDesired(id, recorder.DesiredRunning)
		return m.view(rec), err
	}
	if err := m.store.SetDesired(id, recorder.DesiredRunning); err != nil {
		return JobView{}, err
	}
	return m.view(rec), nil
}

// Stop halts a recording and marks its desired state stopped.
func (m *Manager) Stop(id string) (JobView, error) {
	m.mu.Lock()
	rec, ok := m.recorders[id]
	m.mu.Unlock()
	if !ok {
		return JobView{}, ErrNotFound
	}
	// Persist the stopped intent before waiting for the recorder to drain. A
	// high-volume subscription can take time to finish its in-flight work; if
	// the process is restarted during that window it must not reconnect merely
	// because the old running intent was still stored.
	previousDesired := rec.Spec().Desired
	rec.SetDesired(recorder.DesiredStopped)
	if err := m.store.SetDesired(id, recorder.DesiredStopped); err != nil {
		rec.SetDesired(previousDesired)
		return JobView{}, err
	}
	rec.Stop()
	m.flush(rec)
	return m.view(rec), nil
}

// Presets returns the configured NATS environments (snapshot copy).
func (m *Manager) Presets() map[string]natspreset.Preset {
	m.presetMu.RLock()
	defer m.presetMu.RUnlock()
	out := make(map[string]natspreset.Preset, len(m.presets))
	for k, v := range m.presets {
		out[k] = v
	}
	return out
}

// Shutdown stops every running recorder and tears down all taps.
func (m *Manager) Shutdown() {
	m.stopAllTaps()

	m.mu.RLock()
	recs := make([]*recorder.Recorder, 0, len(m.recorders))
	for _, rec := range m.recorders {
		recs = append(recs, rec)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, rec := range recs {
		wg.Add(1)
		go func(r *recorder.Recorder) {
			defer wg.Done()
			r.Stop()
			m.flush(r)
		}(rec)
	}
	wg.Wait()
}

func (m *Manager) startRecorder(rec *recorder.Recorder) error {
	preset, ok := m.resolvePreset(rec.Spec().NATSEnv)
	if !ok {
		return fmt.Errorf("%w: unknown nats env %q", ErrValidate, rec.Spec().NATSEnv)
	}
	return rec.Start(m.rootCtx, m.dsn, preset)
}

func (m *Manager) validate(spec recorder.JobSpec) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.validateLocked(spec)
}

// validateLocked is validate for callers holding m.mu, so the topic cannot be
// re-pointed between the check and the row being written.
func (m *Manager) validateLocked(spec recorder.JobSpec) error {
	tmpl, _, _, err := m.currentProtoSnapshotLocked(spec.Topic)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrValidate, err)
	}
	return m.validateWithTemplateLocked(spec, tmpl)
}

// validateWithTemplateLocked validates a spec against the schema it will
// actually use. Existing recordings pass their pinned template here; new ones
// pass the current active template.
func (m *Manager) validateWithTemplateLocked(spec recorder.JobSpec, tmpl proto.Message) error {
	if err := checkColumnsMatch(tmpl, spec.Columns); err != nil {
		return err
	}
	if _, ok := m.resolvePreset(spec.NATSEnv); !ok {
		return fmt.Errorf("%w: unknown nats env %q", ErrValidate, spec.NATSEnv)
	}
	// The gate constructor is the single source of truth for what sampling
	// configs are valid, so the API and the hot path can never disagree.
	if err := recorder.ValidateSampling(spec, tmpl); err != nil {
		return fmt.Errorf("%w: %v", ErrValidate, err)
	}
	if spec.Table == "" {
		return fmt.Errorf("%w: table name is required", ErrValidate)
	}
	if spec.RetentionHours < 1 || spec.RetentionHours > m.maxRetentionHours {
		return fmt.Errorf("%w: retention_hours must be 1-%d — every recording needs a retention window", ErrValidate, m.maxRetentionHours)
	}
	return nil
}

// checkColumnsMatch rejects columns that do not describe the resolved type. The
// console picks them when the form opens; if the mapping moved on since, a
// missing field would silently write NULL forever, and a field that merely
// kept its name across types (ExampleText.id is TEXT, ExampleNumber.id is BIGINT)
// would take the wrong column type and fail or misstore every insert.
func checkColumnsMatch(msg proto.Message, cols []schema.ColumnDef) error {
	if len(cols) == 0 {
		return nil // the recorder introspects the resolved type itself
	}
	want := make(map[string]string, len(cols))
	for _, c := range schema.Introspect(msg) {
		want[c.Field] = c.Type
	}
	name := msg.ProtoReflect().Descriptor().FullName()
	for _, c := range cols {
		typ, ok := want[c.Field]
		if !ok {
			return fmt.Errorf("%w: column %q names field %q, which %s does not have — reload the page, the topic's proto type changed",
				ErrValidate, c.Column, c.Field, name)
		}
		// An empty type means the client left it to the server to derive.
		if c.Type != "" && !strings.EqualFold(c.Type, typ) {
			return fmt.Errorf("%w: column %q types field %q as %s, but %s declares it %s — reload the page, the topic's proto type changed",
				ErrValidate, c.Column, c.Field, c.Type, name, typ)
		}
	}
	return nil
}

// checkDuplicateTable must be called with m.mu held. exceptID is skipped (used
// on Update so a job doesn't collide with itself).
func (m *Manager) checkDuplicateTable(table, exceptID string) error {
	for id, rec := range m.recorders {
		if id == exceptID {
			continue
		}
		if rec.Spec().Table == table {
			return fmt.Errorf("%w: %q", ErrDuplicate, table)
		}
	}
	return nil
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
