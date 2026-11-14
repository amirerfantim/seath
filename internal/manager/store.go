package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"seath/internal/config"

	"seath/internal/natspreset"
	"seath/internal/prototypes"
	"seath/internal/recorder"
	"seath/internal/schema"
	"seath/internal/secretbox"

	"github.com/lib/pq"
)

// Store persists job definitions and their session counters in the
// recorder_jobs table in the application database, so recordings (and their totals) survive
// process restarts.
type Store struct {
	db *sql.DB
	// box encrypts preset passwords and tokens at rest. Nil means no
	// SEATH_SECRET_KEY: new values are stored as plaintext and encrypted
	// values cannot be read.
	box *secretbox.Box
}

const createJobsTable = `
CREATE TABLE IF NOT EXISTS recorder_jobs (
	id            TEXT PRIMARY KEY,
	topic         TEXT NOT NULL,
	table_name    TEXT NOT NULL,
	sample_rate   INTEGER NOT NULL,
	nats_env      TEXT NOT NULL,
	columns       JSONB NOT NULL,
	desired_state TEXT NOT NULL DEFAULT 'stopped',
	received      BIGINT NOT NULL DEFAULT 0,
	written       BIGINT NOT NULL DEFAULT 0,
	errors        BIGINT NOT NULL DEFAULT 0,
	created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

// alterAddCounters upgrades pre-existing recorder_jobs tables (deployed before
// counters / retention / indexing were persisted).
const alterAddCounters = `
ALTER TABLE recorder_jobs
	ADD COLUMN IF NOT EXISTS received       BIGINT  NOT NULL DEFAULT 0,
	ADD COLUMN IF NOT EXISTS written        BIGINT  NOT NULL DEFAULT 0,
	ADD COLUMN IF NOT EXISTS errors         BIGINT  NOT NULL DEFAULT 0,
	ADD COLUMN IF NOT EXISTS retention_days  INTEGER NOT NULL DEFAULT 0,
	ADD COLUMN IF NOT EXISTS retention_hours INTEGER NOT NULL DEFAULT 0,
	ADD COLUMN IF NOT EXISTS index_time      BOOLEAN NOT NULL DEFAULT false,
	ADD COLUMN IF NOT EXISTS created_by      TEXT    NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS sample_mode     TEXT    NOT NULL DEFAULT 'rate',
	ADD COLUMN IF NOT EXISTS sample_field    TEXT    NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS sample_values   JSONB   NOT NULL DEFAULT '[]',
	ADD COLUMN IF NOT EXISTS sample_filters  JSONB   NOT NULL DEFAULT '[]',
	ADD COLUMN IF NOT EXISTS proto_name       TEXT    NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS proto_module_ids BIGINT[] NOT NULL DEFAULT '{}';`

// unbounded stands for a retention a pre-v1.1 image recorded as "keep forever".
// It is always larger than any configured ceiling, so it clamps down to it.
const unbounded = math.MaxInt

// reconcileRetention decides a recording's retention window from the two stored
// columns. Retention is mandatory now, so the result is always between 1 and
// maxHours — nothing is ever left unbounded.
//
// This version keeps the columns consistent (hours ≥ 1 ⇔ days == 1), so any
// other combination was last written by a pre-v1.1 image and is reinterpreted:
//
//	days > 1                →  a multi-day window; the shim only ever writes 1
//	days == 1, hours == 0   →  a legacy one-day window
//	days == 0, hours > 0    →  that image was told "keep forever"
//	days == 0, hours == 0   →  never configured, i.e. also forever
//
// shortened reports that the stored intent did not survive, so the caller can
// warn before rows start being deleted.
//
// One case is genuinely ambiguous: a rolled-back image setting exactly one day
// looks identical to a v1.1 row (days == 1). It is resolved in favour of the
// stored hours, which can only retain less than the operator asked for, never
// more than the ceiling allows.
func reconcileRetention(days, hours, maxHours int) (newHours, newDays int, shortened bool) {
	if maxHours < 1 {
		maxHours = config.DefaultMaxRetentionHours
	}

	var wanted int
	switch {
	case days > 1:
		wanted = legacyDaysToHours(days)
	case days == 0:
		wanted = unbounded // keep-forever, or never configured
	case hours <= 0:
		wanted = 24 // days == 1
	default:
		wanted = hours
	}

	newHours = min(wanted, maxHours)
	if newHours < 1 {
		newHours = 1
	}
	return newHours, retentionDaysShim(newHours), wanted > newHours
}

// legacyDaysToHours converts a pre-v1.1 window without overflowing.
func legacyDaysToHours(days int) int {
	if days > math.MaxInt/24 {
		return unbounded
	}
	return days * 24
}

// convertRetention brings both retention columns back in step, applying
// reconcileRetention to every recording. Shortening a window deletes rows the
// operator expected to keep, so it is announced before it is applied — a crash
// in between then leaves the warning already emitted and the work still to do.
func convertRetention(db *sql.DB, maxHours int) error {
	type change struct {
		id       string
		table    string
		days     int
		hours    int
		newHours int
		newDays  int
	}

	rows, err := db.Query(`SELECT id, table_name, retention_days, retention_hours FROM recorder_jobs`)
	if err != nil {
		return fmt.Errorf("inspect retention: %w", err)
	}
	var changes []change
	var shortened []string
	for rows.Next() {
		var c change
		if err := rows.Scan(&c.id, &c.table, &c.days, &c.hours); err != nil {
			rows.Close()
			return fmt.Errorf("inspect retention: %w", err)
		}
		newHours, newDays, wasShortened := reconcileRetention(c.days, c.hours, maxHours)
		if newHours == c.hours && newDays == c.days {
			continue
		}
		c.newHours, c.newDays = newHours, newDays
		changes = append(changes, c)
		if wasShortened {
			shortened = append(shortened, fmt.Sprintf("%s (was %s → %dh)", c.table, describeStored(c.days, c.hours), newHours))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect retention: %w", err)
	}
	if len(changes) == 0 {
		return nil
	}

	if len(shortened) > 0 {
		slog.Warn("retention shortened to the configured maximum; rows older than that will be deleted on the next cleanup",
			"recordings", strings.Join(shortened, ", "), "max_hours", maxHours)
	}
	for _, c := range changes {
		if _, err := db.Exec(
			`UPDATE recorder_jobs SET retention_hours = $2, retention_days = $3 WHERE id = $1`,
			c.id, c.newHours, c.newDays); err != nil {
			return fmt.Errorf("convert retention for %s: %w", c.table, err)
		}
		slog.Info("retention reconciled", "table", c.table, "hours", c.newHours)
	}
	return nil
}

// PersistedJob is a job spec plus its last-saved session counters.
type PersistedJob struct {
	Spec     recorder.JobSpec
	Received uint64
	Written  uint64
	Errors   uint64
}

const createCustomTopicsTable = `
CREATE TABLE IF NOT EXISTS custom_topics (
	topic      TEXT PRIMARY KEY,
	proto_name TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

const createPresetsTable = `
CREATE TABLE IF NOT EXISTS recorder_presets (
	name       TEXT PRIMARY KEY,
	url        TEXT NOT NULL,
	username   TEXT NOT NULL DEFAULT '',
	password   TEXT NOT NULL DEFAULT '',
	token      TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

const createProtoModulesTable = `
CREATE TABLE IF NOT EXISTS proto_modules (
	id             BIGSERIAL PRIMARY KEY,
	module_path    TEXT NOT NULL,
	version        TEXT NOT NULL,
	descriptor_set BYTEA NOT NULL,
	sha256         TEXT NOT NULL,
	active         BOOLEAN NOT NULL DEFAULT false,
	created_by     TEXT NOT NULL DEFAULT '',
	created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	UNIQUE (module_path, version, sha256)
);`

const createActiveProtoModuleIndex = `
CREATE UNIQUE INDEX IF NOT EXISTS proto_modules_one_active_version
	ON proto_modules (module_path) WHERE active;`

// NewStore ensures the recorder_jobs and custom_topics tables exist and are up
// to date.
func NewStore(db *sql.DB, maxRetentionHours int) (*Store, error) {
	if _, err := db.Exec(createJobsTable); err != nil {
		return nil, fmt.Errorf("create recorder_jobs: %w", err)
	}
	if _, err := db.Exec(alterAddCounters); err != nil {
		return nil, fmt.Errorf("upgrade recorder_jobs: %w", err)
	}
	if err := convertRetention(db, maxRetentionHours); err != nil {
		return nil, err
	}
	if _, err := db.Exec(createCustomTopicsTable); err != nil {
		return nil, fmt.Errorf("create custom_topics: %w", err)
	}
	if _, err := db.Exec(createPresetsTable); err != nil {
		return nil, fmt.Errorf("create recorder_presets: %w", err)
	}
	if _, err := db.Exec(createProtoModulesTable); err != nil {
		return nil, fmt.Errorf("create proto_modules: %w", err)
	}
	if _, err := db.Exec(createActiveProtoModuleIndex); err != nil {
		return nil, fmt.Errorf("index active proto modules: %w", err)
	}
	return &Store{db: db}, nil
}

const selectProtoModules = `SELECT id, module_path, version, descriptor_set, sha256, active, created_by, created_at FROM proto_modules`

func scanProtoModules(rows *sql.Rows) ([]prototypes.Module, error) {
	var modules []prototypes.Module
	for rows.Next() {
		var module prototypes.Module
		if err := rows.Scan(&module.ID, &module.Path, &module.Version, &module.DescriptorSet, &module.SHA256, &module.Active, &module.CreatedBy, &module.CreatedAt); err != nil {
			return nil, err
		}
		modules = append(modules, module)
	}
	return modules, rows.Err()
}

// ListProtoModules returns every imported version, newest first.
func (s *Store) ListProtoModules() ([]prototypes.Module, error) {
	rows, err := s.db.Query(selectProtoModules + ` ORDER BY module_path, created_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProtoModules(rows)
}

// LoadActiveProtoModules returns the one active version of each module.
func (s *Store) LoadActiveProtoModules() ([]prototypes.Module, error) {
	rows, err := s.db.Query(selectProtoModules + ` WHERE active ORDER BY module_path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProtoModules(rows)
}

func (s *Store) LoadProtoModule(id int64) (prototypes.Module, error) {
	var module prototypes.Module
	err := s.db.QueryRow(selectProtoModules+` WHERE id = $1`, id).Scan(
		&module.ID, &module.Path, &module.Version, &module.DescriptorSet, &module.SHA256,
		&module.Active, &module.CreatedBy, &module.CreatedAt)
	return module, err
}

// LoadProtoModules loads the exact immutable catalogue snapshot referenced by
// a recording. Missing IDs are rejected instead of silently building a partial
// decoder.
func (s *Store) LoadProtoModules(ids []int64) ([]prototypes.Module, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("proto snapshot has no module IDs")
	}
	rows, err := s.db.Query(selectProtoModules+` WHERE id = ANY($1) ORDER BY module_path`, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	modules, err := scanProtoModules(rows)
	if err != nil {
		return nil, err
	}
	if len(modules) != len(ids) {
		return nil, fmt.Errorf("proto snapshot references %d modules but %d exist", len(ids), len(modules))
	}
	return modules, nil
}

// SaveProtoModule stores an immutable descriptor bundle. Re-importing the same
// module/version/content returns the existing row instead of creating copies.
func (s *Store) SaveProtoModule(module prototypes.Module) (prototypes.Module, error) {
	err := s.db.QueryRow(`
		INSERT INTO proto_modules (module_path, version, descriptor_set, sha256, created_by)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (module_path, version, sha256) DO UPDATE
		SET descriptor_set = proto_modules.descriptor_set
		RETURNING id, active, created_at`,
		module.Path, module.Version, module.DescriptorSet, module.SHA256, module.CreatedBy,
	).Scan(&module.ID, &module.Active, &module.CreatedAt)
	return module, err
}

// ActivateProtoModule atomically replaces the active version for one module.
func (s *Store) ActivateProtoModule(id int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var path string
	if err := tx.QueryRow(`SELECT module_path FROM proto_modules WHERE id = $1 FOR UPDATE`, id).Scan(&path); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE proto_modules SET active = false WHERE module_path = $1 AND active`, path); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE proto_modules SET active = true WHERE id = $1`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// presetAAD binds a stored secret to its preset and column, so ciphertext
// copied into another row or column does not decrypt.
func presetAAD(name, column string) string {
	return "recorder_presets/" + name + "/" + column
}

// sealPreset returns the password and token as they are written to the
// database: encrypted when a key is configured, plaintext otherwise.
func (s *Store) sealPreset(p natspreset.Preset) (password, token string, err error) {
	if s.box == nil {
		return p.Password, p.Token, nil
	}
	if password, err = s.box.Seal(p.Password, presetAAD(p.Name, "password")); err != nil {
		return "", "", err
	}
	if token, err = s.box.Seal(p.Token, presetAAD(p.Name, "token")); err != nil {
		return "", "", err
	}
	return password, token, nil
}

// LoadPresets returns all DB-backed NATS environment presets, decrypted.
func (s *Store) LoadPresets() ([]natspreset.Preset, error) {
	rows, err := s.db.Query(`SELECT name, url, username, password, token FROM recorder_presets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []natspreset.Preset
	for rows.Next() {
		var p natspreset.Preset
		if err := rows.Scan(&p.Name, &p.URL, &p.Username, &p.Password, &p.Token); err != nil {
			return nil, err
		}
		if p.Password, err = s.box.Open(p.Password, presetAAD(p.Name, "password")); err != nil {
			return nil, fmt.Errorf("preset %q password: %w", p.Name, err)
		}
		if p.Token, err = s.box.Open(p.Token, presetAAD(p.Name, "token")); err != nil {
			return nil, fmt.Errorf("preset %q token: %w", p.Name, err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// AddPreset upserts a DB-backed NATS environment preset.
func (s *Store) AddPreset(p natspreset.Preset) error {
	password, token, err := s.sealPreset(p)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO recorder_presets (name, url, username, password, token) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (name) DO UPDATE SET url = EXCLUDED.url, username = EXCLUDED.username, password = EXCLUDED.password, token = EXCLUDED.token`,
		p.Name, p.URL, p.Username, password, token)
	return err
}

// CreatePreset atomically inserts a preset, refusing existing names.
func (s *Store) CreatePreset(p natspreset.Preset) error {
	password, token, err := s.sealPreset(p)
	if err != nil {
		return err
	}
	result, err := s.db.Exec(`INSERT INTO recorder_presets (name, url, username, password, token)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (name) DO NOTHING`, p.Name, p.URL, p.Username, password, token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrDuplicate
	}
	return nil
}

// UseSecretKey makes the store encrypt preset secrets with box, and encrypts
// every preset password and token that is still plaintext. The rewrite runs in
// one transaction, so a failure leaves the table as it was.
func (s *Store) UseSecretKey(box *secretbox.Box) (encrypted int, err error) {
	s.box = box
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(`SELECT name, password, token FROM recorder_presets FOR UPDATE`)
	if err != nil {
		return 0, err
	}
	type pending struct{ name, password, token string }
	var todo []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.name, &p.password, &p.token); err != nil {
			rows.Close()
			return 0, err
		}
		if needsSealing(p.password) || needsSealing(p.token) {
			todo = append(todo, p)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, p := range todo {
		password, token := p.password, p.token
		if needsSealing(password) {
			if password, err = box.Seal(password, presetAAD(p.name, "password")); err != nil {
				return 0, err
			}
		}
		if needsSealing(token) {
			if token, err = box.Seal(token, presetAAD(p.name, "token")); err != nil {
				return 0, err
			}
		}
		if _, err := tx.Exec(`UPDATE recorder_presets SET password = $2, token = $3 WHERE name = $1`, p.name, password, token); err != nil {
			return 0, err
		}
	}
	return len(todo), tx.Commit()
}

// needsSealing reports whether a stored secret is non-empty plaintext.
func needsSealing(v string) bool { return v != "" && !secretbox.IsEncrypted(v) }

// CountPlaintextPresetSecrets counts presets whose password or token is stored
// unencrypted, so boot can warn while SEATH_SECRET_KEY is unset.
func (s *Store) CountPlaintextPresetSecrets() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM recorder_presets
		WHERE (password <> '' AND password NOT LIKE 'enc:v1:%') OR (token <> '' AND token NOT LIKE 'enc:v1:%')`).Scan(&n)
	return n, err
}

// DeletePreset removes a DB-backed preset.
func (s *Store) DeletePreset(name string) error {
	_, err := s.db.Exec(`DELETE FROM recorder_presets WHERE name = $1`, name)
	return err
}

// LoadCustomTopics returns all runtime topic→proto-name mappings.
func (s *Store) LoadCustomTopics() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT topic, proto_name FROM custom_topics ORDER BY topic`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var topic, name string
		if err := rows.Scan(&topic, &name); err != nil {
			return nil, err
		}
		out[topic] = name
	}
	return out, rows.Err()
}

// mappingSQLTimeout bounds the two mapping statements. The manager holds its
// global write lock across them, so an unresponsive database would otherwise
// stall the recordings list and its SSE stream for as long as the pool waits.
const mappingSQLTimeout = 5 * time.Second

// AddCustomTopic upserts a topic→proto-name mapping.
func (s *Store) AddCustomTopic(topic, protoName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mappingSQLTimeout)
	defer cancel()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO custom_topics (topic, proto_name) VALUES ($1, $2)
		 ON CONFLICT (topic) DO UPDATE SET proto_name = EXCLUDED.proto_name`,
		topic, protoName)
	return err
}

// DeleteCustomTopic removes a topic mapping.
func (s *Store) DeleteCustomTopic(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mappingSQLTimeout)
	defer cancel()
	_, err := s.db.ExecContext(ctx, `DELETE FROM custom_topics WHERE topic = $1`, topic)
	return err
}

const selectJobsSQL = `SELECT id, topic, table_name, sample_rate, nats_env, columns, desired_state, received, written, errors, retention_hours, index_time, created_by, created_at, sample_mode, sample_field, sample_values, sample_filters, proto_name, proto_module_ids FROM recorder_jobs ORDER BY created_at`

// Load returns all persisted jobs with their counters.
func (s *Store) Load() ([]PersistedJob, error) {
	rows, err := s.db.Query(selectJobsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []PersistedJob
	for rows.Next() {
		var j PersistedJob
		var colsJSON, valuesJSON, filtersJSON []byte
		var moduleIDs pq.Int64Array
		if err := rows.Scan(&j.Spec.ID, &j.Spec.Topic, &j.Spec.Table, &j.Spec.SampleRate, &j.Spec.NATSEnv, &colsJSON, &j.Spec.Desired, &j.Received, &j.Written, &j.Errors, &j.Spec.RetentionHours, &j.Spec.IndexTime, &j.Spec.CreatedBy, &j.Spec.CreatedAt, &j.Spec.SampleMode, &j.Spec.SampleField, &valuesJSON, &filtersJSON, &j.Spec.ProtoName, &moduleIDs); err != nil {
			return nil, err
		}
		j.Spec.ProtoModuleIDs = append([]int64(nil), moduleIDs...)
		if len(colsJSON) > 0 {
			if err := json.Unmarshal(colsJSON, &j.Spec.Columns); err != nil {
				return nil, fmt.Errorf("decode columns for %s: %w", j.Spec.ID, err)
			}
		}
		if len(valuesJSON) > 0 {
			if err := json.Unmarshal(valuesJSON, &j.Spec.SampleValues); err != nil {
				return nil, fmt.Errorf("decode sample_values for %s: %w", j.Spec.ID, err)
			}
		}
		if len(filtersJSON) > 0 {
			if err := json.Unmarshal(filtersJSON, &j.Spec.SampleFilters); err != nil {
				return nil, fmt.Errorf("decode sample_filters for %s: %w", j.Spec.ID, err)
			}
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// retentionDaysShim keeps the legacy retention_days column meaningful for a
// pre-v1.1 image: any retention at all reads there as one day, never as
// "keep forever", so a rollback degrades to a coarser window instead of
// silently disabling cleanup.
func retentionDaysShim(hours int) int {
	if hours <= 0 {
		return 0
	}
	return 1
}

// created_by/created_at are insert-only: an edit must not reassign ownership.
const upsertJobSQL = `
	INSERT INTO recorder_jobs (id, topic, table_name, sample_rate, nats_env, columns, desired_state, received, written, errors, retention_hours, retention_days, index_time, created_by, created_at, sample_mode, sample_field, sample_values, sample_filters, proto_name, proto_module_ids, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, 0, 0, 0, $8, $12, $9, $10, $11, $13, $14, $15::jsonb, $16::jsonb, $17, $18, NOW())
	ON CONFLICT (id) DO UPDATE SET
		topic = EXCLUDED.topic,
		table_name = EXCLUDED.table_name,
		sample_rate = EXCLUDED.sample_rate,
		nats_env = EXCLUDED.nats_env,
		columns = EXCLUDED.columns,
		desired_state = EXCLUDED.desired_state,
		received = 0, written = 0, errors = 0,
		retention_hours = EXCLUDED.retention_hours,
		retention_days = EXCLUDED.retention_days,
		index_time = EXCLUDED.index_time,
		sample_mode = EXCLUDED.sample_mode,
		sample_field = EXCLUDED.sample_field,
		sample_values = EXCLUDED.sample_values,
		sample_filters = EXCLUDED.sample_filters,
		proto_name = EXCLUDED.proto_name,
		proto_module_ids = EXCLUDED.proto_module_ids,
		updated_at = NOW()`

// Save upserts a job spec, resetting counters (a new or edited job starts its
// totals fresh; counters are advanced separately via SaveCounters).
func (s *Store) Save(spec recorder.JobSpec) error {
	sampling, err := recorder.NormalizeSampling(spec.Sampling())
	if err != nil {
		return err
	}
	cols := spec.Columns
	if cols == nil {
		cols = []schema.ColumnDef{}
	}
	colsJSON, err := json.Marshal(cols)
	if err != nil {
		return err
	}
	valuesJSON, err := json.Marshal([]string{})
	if err != nil {
		return err
	}
	filters := sampling.Filters
	if filters == nil {
		filters = []recorder.SamplingFilter{}
	}
	filtersJSON, err := json.Marshal(filters)
	if err != nil {
		return err
	}
	createdAt := spec.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	_, err = s.db.Exec(upsertJobSQL,
		spec.ID, spec.Topic, spec.Table, sampling.Rate, spec.NATSEnv, string(colsJSON), spec.Desired, spec.RetentionHours, spec.IndexTime, spec.CreatedBy, createdAt, retentionDaysShim(spec.RetentionHours), sampling.Mode, "", string(valuesJSON), string(filtersJSON), spec.ProtoName, pq.Array(spec.ProtoModuleIDs))
	return err
}

// SetProtoSnapshot backfills a legacy recording without resetting counters or
// changing any other part of its persisted definition. Repeating it is safe.
func (s *Store) SetProtoSnapshot(id, protoName string, moduleIDs []int64) error {
	result, err := s.db.Exec(`UPDATE recorder_jobs
		SET proto_name = $2, proto_module_ids = $3, updated_at = NOW()
		WHERE id = $1`,
		id, protoName, pq.Array(moduleIDs))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("recording %q not found while storing proto snapshot", id)
	}
	return nil
}

// SaveCounters persists the latest session counters for a job.
func (s *Store) SaveCounters(id string, received, written, errors uint64) error {
	_, err := s.db.Exec(
		`UPDATE recorder_jobs SET received = $2, written = $3, errors = $4, updated_at = NOW() WHERE id = $1`,
		id, received, written, errors)
	return err
}

// SetSampling updates only a job's sampling rule, preserving counters — the
// rule can change on a running recording without resetting its totals.
func (s *Store) SetSampling(id string, sampling recorder.Sampling) error {
	normalized, err := recorder.NormalizeSampling(sampling)
	if err != nil {
		return err
	}
	valuesJSON, err := json.Marshal([]string{})
	if err != nil {
		return err
	}
	filters := normalized.Filters
	if filters == nil {
		filters = []recorder.SamplingFilter{}
	}
	filtersJSON, err := json.Marshal(filters)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`UPDATE recorder_jobs SET sample_mode = $2, sample_rate = $3, sample_field = '', sample_values = $4::jsonb, sample_filters = $5::jsonb, updated_at = NOW() WHERE id = $1`,
		id, normalized.Mode, normalized.Rate, string(valuesJSON), string(filtersJSON))
	return err
}

// SetDesired updates only the desired_state of a job.
func (s *Store) SetDesired(id, state string) error {
	_, err := s.db.Exec(`UPDATE recorder_jobs SET desired_state = $2, updated_at = NOW() WHERE id = $1`, id, state)
	return err
}

// Delete removes a job spec.
func (s *Store) Delete(id string) error {
	_, err := s.db.Exec(`DELETE FROM recorder_jobs WHERE id = $1`, id)
	return err
}

// describeStored renders what a pre-v1.1 row had recorded, for the warning.
func describeStored(days, hours int) string {
	switch {
	case days == 0 && hours == 0:
		return "unset (kept forever)"
	case days == 0:
		return fmt.Sprintf("keep forever (stale %dh)", hours)
	case hours == 0:
		return fmt.Sprintf("%dd", days)
	default:
		return fmt.Sprintf("%dd (stale %dh)", days, hours)
	}
}
