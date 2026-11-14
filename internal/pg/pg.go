package pg

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"regexp"

	"github.com/lib/pq"

	"seath/internal/schema"
)

// identPattern is what a table or column name may look like: a plain SQL
// identifier, at most 63 bytes. Proto field names always match it.
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// tablePattern is stricter for recording tables: lower case only, because
// most queries quote the name and so do not fold case.
var tablePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// columnTypes are the only column types EnsureTable writes. They are the
// types schema.Introspect produces.
var columnTypes = map[string]bool{"BIGINT": true, "BOOLEAN": true, "DOUBLE PRECISION": true, "INTEGER": true, "JSONB": true, "TEXT": true, "TIMESTAMPTZ": true}

// ValidTableName reports whether name may be used as a recording table.
func ValidTableName(name string) bool { return tablePattern.MatchString(name) }

// ValidColumnName reports whether name may be used as a recording column.
func ValidColumnName(name string) bool { return identPattern.MatchString(name) }

// IsProtectedTable reports whether name is one of Seath's own tables.
func IsProtectedTable(name string) bool {
	for _, t := range ProtectedTables {
		if strings.EqualFold(t, name) {
			return true
		}
	}
	return false
}

// ident validates name and returns it quoted. It lower-cases first, which is
// exactly how Postgres resolves the unquoted names older versions wrote, so
// existing tables and columns keep resolving.
func ident(name string) (string, error) {
	if !identPattern.MatchString(name) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return pq.QuoteIdentifier(strings.ToLower(name)), nil
}

// reserved are the column names this package adds itself. A proto field that
// maps to one of these would collide, so NormalizeColumns renames it.
var reserved = map[string]bool{"id": true, "received_at": true, "nats_subject": true}

// NormalizeColumns renames any column that collides with a reserved column
// (id, received_at, nats_subject) by appending "_field". It is idempotent and
// preserves order, so callers can apply it before introspection display and
// EnsureTable/Insert apply it again without drift.
func NormalizeColumns(cols []schema.ColumnDef) []schema.ColumnDef {
	out := make([]schema.ColumnDef, len(cols))
	for i, c := range cols {
		for reserved[strings.ToLower(c.Column)] {
			c.Column += "_field"
		}
		out[i] = c
	}
	return out
}

func Connect(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	return db, nil
}

// EnsureTable creates the table if it does not exist, plus any requested
// indexes. Schema: id (PK), received_at, nats_subject, then one column per
// included ColumnDef. If indexTime is set, an index is created on received_at;
// columns with Index set get their own index too. All index creation is
// IF NOT EXISTS, so this is safe to call repeatedly.
func EnsureTable(db *sql.DB, table string, cols []schema.ColumnDef, indexTime bool) error {
	cols = NormalizeColumns(cols)
	qt, err := ident(table)
	if err != nil {
		return err
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE TABLE IF NOT EXISTS %s (\n", qt)
	sb.WriteString("  id          BIGSERIAL    PRIMARY KEY,\n")
	sb.WriteString("  received_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),\n")
	sb.WriteString("  nats_subject TEXT         NOT NULL")
	for _, col := range cols {
		if !col.Include {
			continue
		}
		qc, err := ident(col.Column)
		if err != nil {
			return err
		}
		typ := strings.ToUpper(strings.TrimSpace(col.Type))
		if !columnTypes[typ] {
			return fmt.Errorf("invalid column type %q for %q", col.Type, col.Column)
		}
		fmt.Fprintf(&sb, ",\n  %s %s", qc, typ)
	}
	sb.WriteString("\n);")
	if _, err := db.Exec(sb.String()); err != nil {
		return err
	}

	if indexTime {
		if err := ensureIndex(db, table, "received_at"); err != nil {
			return err
		}
	}
	for _, col := range cols {
		if col.Include && col.Index {
			if err := ensureIndex(db, table, col.Column); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureIndex(db *sql.DB, table, column string) error {
	qt, err := ident(table)
	if err != nil {
		return err
	}
	qc, err := ident(column)
	if err != nil {
		return err
	}
	idx := strings.ToLower(fmt.Sprintf("idx_%s_%s", table, column))
	_, err = db.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", pq.QuoteIdentifier(idx), qt, qc))
	return err
}

// deleteOlderThanSQL builds the retention delete for a table. The cutoff is a
// bound parameter; only the identifier is interpolated, quoted.
func deleteOlderThanSQL(table string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE received_at < NOW() - ($1 || ' hours')::interval",
		pq.QuoteIdentifier(table))
}

// DeleteOlderThan removes rows whose received_at is older than hours. Returns
// the number of rows deleted. hours <= 0 is a no-op.
func DeleteOlderThan(db *sql.DB, table string, hours int) (int64, error) {
	if hours <= 0 {
		return 0, nil
	}
	res, err := db.Exec(deleteOlderThanSQL(table), hours)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Row is one buffered message awaiting batch insertion.
type Row struct {
	Subject string
	Vals    []interface{}
}

// InsertBatch writes many rows in a single multi-row INSERT — far cheaper than
// one round-trip per message. cols must match the order of each Row.Vals
// (as produced by schema.ExtractValues). received_at is set to NOW() per row.
func InsertBatch(db *sql.DB, table string, cols []schema.ColumnDef, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	cols = NormalizeColumns(cols)

	included := make([]schema.ColumnDef, 0, len(cols))
	valIdx := make([]int, 0, len(cols))
	for i, c := range cols {
		if c.Include {
			included = append(included, c)
			valIdx = append(valIdx, i)
		}
	}

	qt, err := ident(table)
	if err != nil {
		return err
	}
	colNames := make([]string, 0, len(included)+1)
	colNames = append(colNames, "nats_subject")
	for _, c := range included {
		qc, err := ident(c.Column)
		if err != nil {
			return err
		}
		colNames = append(colNames, qc)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (received_at, %s) VALUES ", qt, strings.Join(colNames, ", "))
	args := make([]interface{}, 0, len(rows)*(len(included)+1))
	p := 1
	for ri, row := range rows {
		if ri > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(NOW()")
		fmt.Fprintf(&sb, ", $%d", p)
		args = append(args, row.Subject)
		p++
		for k, c := range included {
			if c.Type == "JSONB" {
				fmt.Fprintf(&sb, ", $%d::jsonb", p)
			} else {
				fmt.Fprintf(&sb, ", $%d", p)
			}
			args = append(args, row.Vals[valIdx[k]])
			p++
		}
		sb.WriteString(")")
	}
	_, err = db.Exec(sb.String(), args...)
	return err
}

// TableStats returns the row count and on-disk size of a table. The row count
// uses the planner estimate (pg_class.reltuples) — instant regardless of table
// size — and only falls back to an exact COUNT(*) when the table has never been
// analyzed (estimate <= 0), e.g. a brand-new table. estimated reports which
// path was used. If the table does not exist yet, it returns zeroes.
func TableStats(db *sql.DB, table string) (rows int64, estimated bool, sizePretty string, sizeBytes int64, err error) {
	var reg sql.NullString
	if err = db.QueryRow(`SELECT to_regclass($1)`, table).Scan(&reg); err != nil {
		return 0, false, "0 bytes", 0, err
	}
	if !reg.Valid {
		return 0, false, "0 bytes", 0, nil
	}
	if err = db.QueryRow(
		`SELECT pg_size_pretty(pg_total_relation_size($1)), pg_total_relation_size($1)`,
		table,
	).Scan(&sizePretty, &sizeBytes); err != nil {
		return 0, false, "0 bytes", 0, err
	}

	var est float64
	if err = db.QueryRow(`SELECT reltuples FROM pg_class WHERE oid = to_regclass($1)`, table).Scan(&est); err != nil {
		return 0, false, sizePretty, sizeBytes, err
	}
	if est > 0 {
		return int64(est), true, sizePretty, sizeBytes, nil
	}
	if err = db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, pq.QuoteIdentifier(table))).Scan(&rows); err != nil {
		return 0, false, sizePretty, sizeBytes, err
	}
	return rows, false, sizePretty, sizeBytes, nil
}

// SampleRows returns the most recent rows of a table (ordered by id desc).
func SampleRows(db *sql.DB, table string, limit int) ([]map[string]interface{}, error) {
	if limit < 1 || limit > 200 {
		limit = 20
	}
	_, rows, err := Rows(db, table, limit, 0, "desc")
	return rows, err
}

// Rows returns a page of a table's rows as generic maps, ordered by id. Byte
// values (e.g. JSONB) are converted to strings. Returns empty if the table does
// not exist. order is "asc" or "desc" (default desc).
func Rows(db *sql.DB, table string, limit, offset int, order string) ([]string, []map[string]interface{}, error) {
	var reg sql.NullString
	if err := db.QueryRow(`SELECT to_regclass($1)`, table).Scan(&reg); err != nil {
		return nil, nil, err
	}
	if !reg.Valid {
		return []string{}, []map[string]interface{}{}, nil
	}
	if limit < 1 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	dir := "DESC"
	if strings.EqualFold(order, "asc") {
		dir = "ASC"
	}

	rows, err := db.Query(fmt.Sprintf(
		`SELECT * FROM %s ORDER BY id %s LIMIT %d OFFSET %d`,
		pq.QuoteIdentifier(table), dir, limit, offset))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	out := make([]map[string]interface{}, 0, limit)
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		m := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[c] = string(b)
			} else {
				m[c] = vals[i]
			}
		}
		out = append(out, m)
	}
	return cols, out, rows.Err()
}

// Query runs an arbitrary user query in a READ ONLY transaction with a
// configurable statement timeout and row cap. truncated reports whether at
// least one additional row was available beyond maxRows.
//
// The query is prepared before it runs. Prepared statements use the extended
// query protocol, which accepts exactly one statement, so input such as
// "COMMIT; DROP TABLE t" is rejected instead of ending the read-only
// transaction and running the rest with write access. Do not replace this
// with a plain QueryContext: without arguments lib/pq uses the simple query
// protocol, which runs every statement in the string.
func Query(ctx context.Context, db *sql.DB, query string, timeout time.Duration, maxRows int) (cols []string, out []map[string]interface{}, truncated bool, err error) {
	if maxRows < 1 {
		return nil, nil, false, fmt.Errorf("max rows must be at least 1")
	}
	tx, err := beginReadOnly(ctx, db, timeout)
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, nil, false, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()

	cols, err = rows.Columns()
	if err != nil {
		return nil, nil, false, err
	}

	out = make([]map[string]interface{}, 0, maxRows)
	for rows.Next() {
		if len(out) >= maxRows {
			truncated = true
			break
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return nil, nil, false, err
		}
		m := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[c] = string(b)
			} else {
				m[c] = vals[i]
			}
		}
		out = append(out, m)
	}
	if !truncated {
		if err = rows.Err(); err != nil {
			return nil, nil, false, err
		}
	}
	return cols, out, truncated, nil
}

// StreamTableCSV writes every row in a table as CSV without buffering the
// entire result in application memory. Rows are ordered oldest-first so the
// export is stable and useful as a chronological archive.
func StreamTableCSV(ctx context.Context, db *sql.DB, table string, timeout time.Duration, dst io.Writer) error {
	tx, err := beginReadOnly(ctx, db, timeout)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, tableExportSQL(table))
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	w := csv.NewWriter(dst)
	if err := w.Write(cols); err != nil {
		return err
	}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		record := make([]string, len(vals))
		for i, value := range vals {
			record[i] = csvValue(value)
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return tx.Commit()
}

func beginReadOnly(ctx context.Context, db *sql.DB, timeout time.Duration) (*sql.Tx, error) {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SELECT set_config('statement_timeout', $1, true)", strconv.FormatInt(timeout.Milliseconds(), 10)); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func tableExportSQL(table string) string {
	return fmt.Sprintf("SELECT * FROM %s ORDER BY id ASC", pq.QuoteIdentifier(table))
}

func csvValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

// ColumnInfo describes a table column.
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// TableColumns returns the columns of a table in ordinal order.
func TableColumns(db *sql.DB, table string) ([]ColumnInfo, error) {
	rows, err := db.Query(
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
		table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// TimeSpan returns the received_at of the first and last rows (by id, which is
// insertion order) — an index-backed approximation of the table's time range.
func TimeSpan(db *sql.DB, table string) (first, last sql.NullTime, err error) {
	var reg sql.NullString
	if err = db.QueryRow(`SELECT to_regclass($1)`, table).Scan(&reg); err != nil || !reg.Valid {
		return first, last, err
	}
	q := pq.QuoteIdentifier(table)
	if err = db.QueryRow(fmt.Sprintf(`SELECT received_at FROM %s ORDER BY id ASC LIMIT 1`, q)).Scan(&first); err != nil && err != sql.ErrNoRows {
		return first, last, err
	}
	if err = db.QueryRow(fmt.Sprintf(`SELECT received_at FROM %s ORDER BY id DESC LIMIT 1`, q)).Scan(&last); err != nil && err != sql.ErrNoRows {
		return first, last, err
	}
	return first, last, nil
}

// DropTable removes a table if it exists.
func DropTable(db *sql.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, pq.QuoteIdentifier(table)))
	return err
}

// PGHostStats is the database host's resource usage, sourced from the optional
// system_stats extension. Available is false when the extension is not installed
// (e.g. a plain Postgres), so callers degrade gracefully.
type PGHostStats struct {
	// Always populated (standard pg_stat_*, no extension needed).
	CacheHitPct float64 `json:"cache_hit_percent"`
	ConnActive  int     `json:"conn_active"`
	ConnTotal   int     `json:"conn_total"`
	ConnMax     int     `json:"conn_max"`

	// Host metrics — populated only when the system_stats extension is present.
	Available   bool    `json:"available"`
	CPUUsedPct  float64 `json:"cpu_used_percent"`
	IOWaitPct   float64 `json:"iowait_percent"`
	Cores       int     `json:"cores"`
	MemTotal    int64   `json:"mem_total_bytes"`
	MemUsed     int64   `json:"mem_used_bytes"`      // total - free (counts reclaimable cache)
	MemUsedReal int64   `json:"mem_used_real_bytes"` // used minus reclaimable page cache — true pressure
	MemFree     int64   `json:"mem_free_bytes"`
	CacheBytes  int64   `json:"cache_bytes"`
	SwapTotal   int64   `json:"swap_total_bytes"`
	SwapUsed    int64   `json:"swap_used_bytes"`
	Load1       float64 `json:"load_1"`
	Load5       float64 `json:"load_5"`
	Load15      float64 `json:"load_15"`
	DiskTotal   int64   `json:"disk_total_bytes"`
	DiskUsed    int64   `json:"disk_used_bytes"`
	DiskFree    int64   `json:"disk_free_bytes"`
	// Cumulative disk I/O across real (non-loop) devices; callers derive a rate.
	DiskReadBytes  int64 `json:"disk_read_bytes"`
	DiskWriteBytes int64 `json:"disk_write_bytes"`
}

// HostStats reads PG health (cache hit, connections — always) plus host
// CPU/memory/disk/IO/load when the system_stats extension is present. Missing
// extension → host fields stay zero and Available is false; no error.
func HostStats(db *sql.DB) (PGHostStats, error) {
	var h PGHostStats

	// Always-available PG health: cache hit ratio + connection counts.
	_ = db.QueryRow(`
		SELECT
			COALESCE((SELECT round((sum(blks_hit)::numeric / NULLIF(sum(blks_hit) + sum(blks_read), 0)) * 100, 2)
			          FROM pg_stat_database), 0),
			(SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
			(SELECT count(*) FROM pg_stat_activity),
			current_setting('max_connections')::int
	`).Scan(&h.CacheHitPct, &h.ConnActive, &h.ConnTotal, &h.ConnMax)

	var present bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'system_stats')`).Scan(&present); err != nil {
		return h, err
	}
	if !present {
		return h, nil
	}
	const q = `SELECT
		COALESCE(100 - c.idle_mode_percent, 0), COALESCE(c.io_completion_percent, 0),
		COALESCE(ci.physical_processor, 0),
		COALESCE(m.total_memory, 0), COALESCE(m.used_memory, 0), COALESCE(m.free_memory, 0),
		COALESCE(m.cache_total, 0), COALESCE(m.swap_total, 0), COALESCE(m.swap_used, 0),
		COALESCE(l.load_avg_one_minute, 0), COALESCE(l.load_avg_five_minutes, 0),
		COALESCE(l.load_avg_fifteen_minutes, l.load_avg_ten_minutes, 0),
		COALESCE(d.total_space, 0), COALESCE(d.used_space, 0), COALESCE(d.free_space, 0),
		COALESCE(io.rd, 0), COALESCE(io.wr, 0)
	FROM pg_sys_cpu_usage_info() c,
	     pg_sys_cpu_info() ci,
	     pg_sys_memory_info() m,
	     pg_sys_load_avg_info() l,
	     (SELECT total_space, used_space, free_space FROM pg_sys_disk_info() ORDER BY total_space DESC LIMIT 1) d,
	     (SELECT sum(read_bytes) AS rd, sum(write_bytes) AS wr FROM pg_sys_io_analysis_info() WHERE device_name NOT LIKE 'loop%') io`
	if err := db.QueryRow(q).Scan(
		&h.CPUUsedPct, &h.IOWaitPct, &h.Cores, &h.MemTotal, &h.MemUsed, &h.MemFree,
		&h.CacheBytes, &h.SwapTotal, &h.SwapUsed, &h.Load1, &h.Load5, &h.Load15,
		&h.DiskTotal, &h.DiskUsed, &h.DiskFree, &h.DiskReadBytes, &h.DiskWriteBytes); err != nil {
		return h, err
	}
	// Page cache is counted as "used" by total-free, but it is reclaimable on
	// demand — so it overstates real memory pressure. Subtract it for the gauge.
	if h.MemUsedReal = h.MemUsed - h.CacheBytes; h.MemUsedReal < 0 {
		h.MemUsedReal = 0
	}
	h.Available = true
	return h, nil
}

// DatabaseSize returns the total on-disk size of the current database.
func DatabaseSize(db *sql.DB) (sizePretty string, sizeBytes int64, err error) {
	err = db.QueryRow(
		`SELECT pg_size_pretty(pg_database_size(current_database())), pg_database_size(current_database())`,
	).Scan(&sizePretty, &sizeBytes)
	return sizePretty, sizeBytes, err
}

// GrantSelectSQL returns the statement that lets the query reader role read
// one recording table. Both identifiers are quoted.
func GrantSelectSQL(table, role string) string {
	return fmt.Sprintf("GRANT SELECT ON %s TO %s", pq.QuoteIdentifier(table), pq.QuoteIdentifier(role))
}

// TableExists reports whether a table with this exact name exists in the
// search path.
func TableExists(db *sql.DB, table string) (bool, error) {
	var exists bool
	err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, pq.QuoteIdentifier(table)).Scan(&exists)
	return exists, err
}

// GrantSelect lets role read table, if the table exists. It returns false,
// nil when the table does not exist yet.
func GrantSelect(db *sql.DB, table, role string) (bool, error) {
	exists, err := TableExists(db, table)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	_, err = db.Exec(GrantSelectSQL(table, role))
	return err == nil, err
}

// ProtectedTables are the application tables the query reader must never be
// able to read or change: credentials, password hashes and recording specs.
var ProtectedTables = []string{"recorder_presets", "recorder_users", "recorder_audit", "recorder_jobs", "custom_topics", "proto_modules"}

// ReaderCheck is what CheckQueryReader learns about the reader login.
type ReaderCheck struct {
	Role     string
	Problems []string
}

// protectedOIDsSQL resolves the protected tables on the MAIN connection, so
// the names resolve with the main role's search_path (the schema the tables
// really live in), not the reader's.
const protectedOIDsSQL = `SELECT t, to_regclass(quote_ident(t))::oid FROM unnest($1::text[]) AS t`

// readerCheckSQL inspects the connected role. It must run on the reader's own
// connection, so current_user is the reader. $1 holds the protected tables'
// OIDs. Membership in ANY other role is reported: a role the reader can SET
// ROLE to (even without inheriting it) lends its privileges to a single
// statement through set_config('role', ...) plus a runtime query such as
// query_to_xml, so the only safe reader is a role with no memberships.
const readerCheckSQL = `
SELECT
	current_user,
	r.rolsuper,
	r.rolbypassrls,
	r.rolcreaterole,
	COALESCE((SELECT array_agg(m.rolname::text ORDER BY m.rolname) FROM pg_roles m
	          WHERE m.rolname <> current_user AND pg_has_role(current_user, m.oid, 'MEMBER')), '{}'),
	COALESCE((SELECT array_agg(t::regclass::text ORDER BY t) FROM unnest($1::oid[]) AS t
	          WHERE has_any_column_privilege(current_user, t, 'SELECT,INSERT,UPDATE,REFERENCES')
	             OR has_table_privilege(current_user, t, 'DELETE,TRUNCATE,TRIGGER')), '{}')
FROM pg_roles r
WHERE r.rolname = current_user`

// CheckQueryReader verifies that reader is a separate, least-privileged login
// for user SQL. main is the application pool; every protected table must
// already exist there. The result lists every problem found; an empty list
// means the reader is safe to use.
func CheckQueryReader(ctx context.Context, main, reader *sql.DB) (ReaderCheck, error) {
	var c ReaderCheck
	mainRole, err := CurrentRole(ctx, main)
	if err != nil {
		return c, fmt.Errorf("read main role: %w", err)
	}
	rows, err := main.QueryContext(ctx, protectedOIDsSQL, pq.StringArray(ProtectedTables))
	if err != nil {
		return c, err
	}
	var oids []int64
	var missing []string
	for rows.Next() {
		var name string
		var oid sql.NullInt64
		if err := rows.Scan(&name, &oid); err != nil {
			rows.Close()
			return c, err
		}
		if oid.Valid {
			oids = append(oids, oid.Int64)
		} else {
			missing = append(missing, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return c, err
	}

	var super, bypassRLS, createRole bool
	var memberOf, tables pq.StringArray
	err = reader.QueryRowContext(ctx, readerCheckSQL, pq.Int64Array(oids)).
		Scan(&c.Role, &super, &bypassRLS, &createRole, &memberOf, &tables)
	if err != nil {
		return c, err
	}
	c.Problems = readerProblems(c.Role, mainRole, super, bypassRLS, createRole, memberOf, tables, missing)
	return c, nil
}

// readerProblems turns the facts about a reader role into human-readable
// reasons it is unsafe. It is pure so the rules can be unit-tested.
func readerProblems(role, mainRole string, super, bypassRLS, createRole bool, memberOf, tables, missing []string) []string {
	var p []string
	if role == mainRole {
		p = append(p, fmt.Sprintf("it connects as %q, the same role as PG_DSN", role))
	}
	if super {
		p = append(p, "it is a superuser")
	}
	if bypassRLS {
		p = append(p, "it has BYPASSRLS")
	}
	if createRole {
		p = append(p, "it has CREATEROLE")
	}
	if len(memberOf) > 0 {
		p = append(p, "it is a member of "+strings.Join(memberOf, ", ")+" and can switch to those roles; the reader must not belong to any role")
	}
	if len(tables) > 0 {
		p = append(p, "it has privileges on "+strings.Join(tables, ", "))
	}
	if len(missing) > 0 {
		p = append(p, "the protected tables "+strings.Join(missing, ", ")+" were not found, so they cannot be checked")
	}
	return p
}

// CurrentRole returns the role a pool connects as.
func CurrentRole(ctx context.Context, db *sql.DB) (string, error) {
	var role string
	err := db.QueryRowContext(ctx, `SELECT current_user`).Scan(&role)
	return role, err
}
