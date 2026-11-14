package recorder

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"seath/internal/config"
	"seath/internal/natspreset"
	"seath/internal/pg"
	"seath/internal/schema"
)

// Status values for a Recorder.
const (
	StatusStopped = "stopped"
	StatusRunning = "running"
	StatusError   = "error"
)

// Desired-state values persisted with a JobSpec.
const (
	DesiredStopped = "stopped"
	DesiredRunning = "running"
)

// JobSpec is the persisted definition of one recording.
type JobSpec struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	Table string `json:"table"`
	// ProtoName and ProtoModuleIDs pin the immutable catalogue snapshot used by
	// this recording. The module IDs are internal persistence metadata; callers
	// may see the resolved type name but cannot choose either value.
	ProtoName      string  `json:"proto_name"`
	ProtoModuleIDs []int64 `json:"-"`
	// SampleMode is retained for persisted backward compatibility. New rules
	// use ModeComposite when SampleFilters is non-empty; legacy exclusive modes
	// are normalized into the equivalent composite rule when loaded.
	SampleMode string `json:"sample_mode"`
	// SampleRate is always the arrival-order percentage applied before payload
	// decoding. Every SampleFilter is then ANDed with that percentage gate.
	SampleRate int `json:"sample_rate"`
	// SampleField and SampleValues are legacy exclusive-mode fields. New clients
	// send SampleFilters; these remain in the API so older stored rows can be
	// decoded and migrated without changing their behavior.
	SampleField  string   `json:"sample_field"`
	SampleValues []string `json:"sample_values"`
	// SampleFilters are evaluated in order and all must match. Values inside one
	// exact filter are alternatives, so that filter matches any listed value.
	SampleFilters  []SamplingFilter   `json:"sample_filters"`
	NATSEnv        string             `json:"nats_env"`
	Columns        []schema.ColumnDef `json:"columns"`
	Desired        string             `json:"desired_state"`
	RetentionHours int                `json:"retention_hours"` // mandatory, 1..configured max
	IndexTime      bool               `json:"index_time"`      // create an index on received_at

	// Set by the manager on create, never taken from a request body; empty for
	// recordings that predate ownership tracking.
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}

// Sampling is the mutable part of a JobSpec's sampling rule, changeable on a
// running recording.
type Sampling struct {
	Mode    string           `json:"sample_mode"`
	Rate    int              `json:"sample_rate"`
	Field   string           `json:"sample_field"`
	Values  []string         `json:"sample_values"`
	Filters []SamplingFilter `json:"sample_filters"`
}

// SamplingFilter is one payload predicate in a composite sampling rule.
// Filters are AND-composed; Values within an exact filter are OR-composed.
type SamplingFilter struct {
	Type     string   `json:"type"`
	Field    string   `json:"field"`
	Values   []string `json:"values,omitempty"`
	Lower    *int     `json:"lower,omitempty"`
	Upper    *int     `json:"upper,omitempty"`
	Operator string   `json:"operator,omitempty"`
	Value    *float64 `json:"value,omitempty"`
}

// Sampling returns the recording's current sampling rule.
func (s JobSpec) Sampling() Sampling {
	return Sampling{Mode: s.SampleMode, Rate: s.SampleRate, Field: s.SampleField, Values: s.SampleValues, Filters: s.SampleFilters}
}

// withSampling returns a copy of the spec with the sampling rule replaced.
func (s JobSpec) withSampling(n Sampling) JobSpec {
	s.SampleMode, s.SampleRate, s.SampleField, s.SampleValues, s.SampleFilters = n.Mode, n.Rate, n.Field, n.Values, n.Filters
	return s
}

// Snapshot is an immutable view of a Recorder's live state for the API.
type Snapshot struct {
	Status     string    `json:"status"`
	Received   uint64    `json:"received"`
	Written    uint64    `json:"written"`
	Errors     uint64    `json:"errors"`
	Dropped    uint64    `json:"dropped"`
	StartedAt  time.Time `json:"started_at"`
	LastError  string    `json:"last_error"`
	NATSStatus string    `json:"nats_status"` // CONNECTED / RECONNECTING / ... (empty if stopped)
	BufferLen  int       `json:"buffer_len"`  // rows queued in the write buffer
	BufferCap  int       `json:"buffer_cap"`  // write-buffer capacity
}

// Resolver maps a topic name to a fresh proto.Message template used to decode
// that topic's payloads. The manager supplies it from the active database
// catalogue and stored topic mappings.
type Resolver func(topic string) (proto.Message, error)

// Recorder owns the lifecycle of a single recording: one NATS subscription
// feeding one Postgres table. It is safe for concurrent use.
type Recorder struct {
	spec    JobSpec
	resolve Resolver

	mu        sync.Mutex
	status    string
	lastError string
	startedAt time.Time

	received atomic.Uint64
	written  atomic.Uint64
	errors   atomic.Uint64
	dropped  atomic.Uint64 // messages discarded because the write buffer was full

	// gate mirrors the spec's sampling rule but is read lock-free in the hot
	// handler path, so the rule can be swapped on the fly while recording.
	// Never nil once New has returned.
	gate atomic.Pointer[gate]

	cancel context.CancelFunc
	done   chan struct{}

	// Live handles for monitoring, set while running (guarded by mu).
	nc    *nats.Conn
	rowCh chan pg.Row
}

// New builds a stopped Recorder for the given spec. resolve maps the topic to
// its proto template from the caller's runtime catalogue.
//
// A spec whose sampling rule cannot be resolved (unknown topic, bad field)
// still yields a usable Recorder that falls back to keeping every message; the
// real rejection happens in Manager.validate before the spec is ever persisted,
// and Start re-resolves the gate against the live template.
func New(spec JobSpec, resolve Resolver) *Recorder {
	if resolve == nil {
		resolve = missingResolver
	}
	tmpl, _ := resolve(spec.Topic)
	return NewResolved(spec, resolve, tmpl)
}

// NewResolved builds a recorder from a template the caller already resolved.
// Manager uses this while holding its own lock, where calling its resolver
// again would recursively acquire that lock.
func NewResolved(spec JobSpec, resolve Resolver, tmpl proto.Message) *Recorder {
	if resolve == nil {
		resolve = missingResolver
	}
	if sampling, err := NormalizeSampling(spec.Sampling()); err == nil {
		spec = spec.withSampling(sampling)
	}
	r := &Recorder{spec: spec, resolve: resolve, status: StatusStopped}
	g, err := newGate(spec, tmpl)
	if err != nil {
		g = &gate{rate: 100}
	}
	r.gate.Store(g)
	return r
}

// NewMessage returns a fresh message from this recording's pinned resolver.
// Manager uses it for edits and live sampling changes so catalogue activation
// cannot silently switch an existing recording to a newer schema.
func (r *Recorder) NewMessage() (proto.Message, error) {
	r.mu.Lock()
	topic := r.spec.Topic
	r.mu.Unlock()
	return r.resolve(topic)
}

func missingResolver(t string) (proto.Message, error) {
	return nil, fmt.Errorf("no proto catalogue resolver configured for topic %q", t)
}

// SetSampling replaces the sampling rule. It takes effect on the next message,
// even while the recording is running, via a single atomic pointer swap. The
// rule is resolved before the swap, so a rejected config leaves the running
// recording untouched.
func (r *Recorder) SetSampling(s Sampling, tmpl proto.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	normalized, err := NormalizeSampling(s)
	if err != nil {
		return err
	}
	spec := r.spec.withSampling(normalized)

	g, err := newGate(spec, tmpl)
	if err != nil {
		return err
	}

	r.gate.Store(g)
	r.spec = spec
	return nil
}

// ValidateSamplingChange checks a prospective live update without changing the
// recorder. Manager uses it before persistence so a bad request changes neither
// Postgres nor the hot-path gate.
func (r *Recorder) ValidateSamplingChange(s Sampling, tmpl proto.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec := r.spec.withSampling(s)
	_, err := newGate(spec, tmpl)
	return err
}

// installCurrentGate recompiles the current spec against a freshly resolved
// template. Holding mu across both the spec read and pointer swap prevents a
// concurrent live update from being overwritten by a start in progress.
func (r *Recorder) installCurrentGate(tmpl proto.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, err := newGate(r.spec, tmpl)
	if err != nil {
		return err
	}
	r.gate.Store(g)
	return nil
}

// SampleDesc renders the active sampling rule for display.
func (r *Recorder) SampleDesc() string { return r.gate.Load().describe() }

// SampleSummary renders a compact sampling label for recording lists.
func (r *Recorder) SampleSummary() string { return r.gate.Load().summary() }

// Spec returns the recorder's definition.
func (r *Recorder) Spec() JobSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spec
}

// SetDesired updates the recorder's persisted desired-state marker.
func (r *Recorder) SetDesired(state string) {
	r.mu.Lock()
	r.spec.Desired = state
	r.mu.Unlock()
}

// SeedCounters initializes the counters from persisted values. Call before
// Start so a restarted recording continues its totals rather than starting
// from zero.
func (r *Recorder) SeedCounters(received, written, errors uint64) {
	r.received.Store(received)
	r.written.Store(written)
	r.errors.Store(errors)
}

// Snapshot returns a consistent view of live state.
func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Snapshot{
		Status:    r.status,
		Received:  r.received.Load(),
		Written:   r.written.Load(),
		Errors:    r.errors.Load(),
		Dropped:   r.dropped.Load(),
		StartedAt: r.startedAt,
		LastError: r.lastError,
	}
	if r.nc != nil {
		s.NATSStatus = r.nc.Status().String()
	}
	if r.rowCh != nil {
		s.BufferLen = len(r.rowCh)
		s.BufferCap = cap(r.rowCh)
	}
	return s
}

func (r *Recorder) setError(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	r.mu.Lock()
	r.status = StatusError
	r.lastError = err.Error()
	r.mu.Unlock()
	return err
}

// Start connects to Postgres and NATS and begins recording. It returns once
// the subscription is live (or fails to come up); the recording runs in the
// background until Stop or parentCtx cancellation.
func (r *Recorder) Start(parentCtx context.Context, dsn string, preset natspreset.Preset) error {
	r.mu.Lock()
	if r.status == StatusRunning {
		r.mu.Unlock()
		return fmt.Errorf("recorder %s already running", r.spec.ID)
	}
	r.mu.Unlock()

	protoTemplate, err := r.resolve(r.spec.Topic)
	if err != nil {
		return r.setError("%v", err)
	}

	// Re-resolve the sampling rule against the live template: New may have
	// fallen back to keep-everything if the topic wasn't resolvable then (for
	// instance a custom mapping added after boot). Failing here is deliberate —
	// silently recording 100% of a topic the user asked to filter is worse than
	// refusing to start.
	if err := r.installCurrentGate(protoTemplate); err != nil {
		return r.setError("sampling: %v", err)
	}

	cols := r.spec.Columns
	if len(cols) == 0 {
		cols = schema.Introspect(protoTemplate)
	}

	db, err := pg.Connect(dsn)
	if err != nil {
		return r.setError("pg connect: %v", err)
	}
	if err := pg.EnsureTable(db, r.spec.Table, cols, r.spec.IndexTime); err != nil {
		db.Close()
		return r.setError("ensure table %q: %v", r.spec.Table, err)
	}

	opts := []nats.Option{
		nats.Name(fmt.Sprintf("seath/%s", r.spec.Topic)),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats disconnected", "id", r.spec.ID, "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("nats reconnected", "id", r.spec.ID, "url", nc.ConnectedUrl())
		}),
	}
	opts = append(opts, natspreset.BuildOptions(preset)...)

	nc, err := nats.Connect(preset.URL, opts...)
	if err != nil {
		db.Close()
		return r.setError("nats connect to %s: %v", preset.URL, err)
	}

	// Buffered hand-off from the NATS callback to the batch writer. Sized to
	// absorb bursts; if it ever fills, the handler drops (tracked) rather than
	// blocking NATS delivery.
	rowCh := make(chan pg.Row, writeBufferSize)

	sub, err := nc.Subscribe(r.spec.Topic, r.handler(protoTemplate, cols, rowCh))
	if err != nil {
		nc.Close()
		db.Close()
		return r.setError("nats subscribe to %q: %v", r.spec.Topic, err)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	done := make(chan struct{})

	r.mu.Lock()
	r.status = StatusRunning
	r.lastError = ""
	r.startedAt = time.Now()
	r.cancel = cancel
	r.done = done
	r.nc = nc
	r.rowCh = rowCh
	// Counters are intentionally not reset here: stopping and restarting a
	// recording continues accumulating rather than zeroing the totals.
	r.mu.Unlock()

	// The write pool drains rowCh from several goroutines so multiple INSERTs run
	// concurrently against Postgres — the single bottleneck behind drops, since
	// one serial writer stalls on per-batch network round-trips while CPU sits
	// idle. Size the pool to allow every worker its own connection.
	db.SetMaxOpenConns(writeWorkers + 2)
	db.SetMaxIdleConns(writeWorkers + 2)

	// Cap the batch so a single multi-row INSERT never exceeds Postgres' 65535
	// bound parameters (params per row = included columns + nats_subject).
	batchSize := safeBatchSize(cols, writeBatchSize)

	slog.Info("recording started", "id", r.spec.ID, "topic", r.spec.Topic,
		"table", r.spec.Table, "sampling", r.SampleDesc(), "env", preset.Name,
		"buffer", writeBufferSize, "batch", batchSize, "workers", writeWorkers)

	go r.writeCoordinator(ctx, db, cols, batchSize, rowCh, sub, nc, done)
	return nil
}

// Batch-writer tuning. Overridable via env so the buffer/throughput can be sized
// to the message rate without a code change or rebuild.
var (
	writeBufferSize = envInt("SEATH_WRITE_BUFFER", 49152, 1024)
	writeBatchSize  = envInt("SEATH_WRITE_BATCH", 2000, 100)
	writeWorkers    = envInt("SEATH_WRITE_WORKERS", 4, 1)
	writeFlushEvery = 250 * time.Millisecond
)

// envInt reads an int env var, falling back to def and clamping to >= min.
func envInt(name string, def, min int) int {
	v := def
	if s := os.Getenv(name); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			v = n
		}
	}
	if v < min {
		v = min
	}
	return v
}

// safeBatchSize lowers the configured batch if the row would push a single
// INSERT past Postgres' 65535-parameter limit.
func safeBatchSize(cols []schema.ColumnDef, configured int) int {
	params := 1 // nats_subject
	for _, c := range cols {
		if c.Include {
			params++
		}
	}
	if params < 1 {
		params = 1
	}
	if max := 60000 / params; configured > max {
		return max
	}
	return configured
}

// writeCoordinator runs the worker pool and owns teardown: on shutdown it stops
// the subscription (so no new rows arrive), waits for workers to drain the
// remaining buffer and flush, then closes NATS and the DB pool once.
func (r *Recorder) writeCoordinator(ctx context.Context, db *sql.DB, cols []schema.ColumnDef, batchSize int, rowCh chan pg.Row, sub *nats.Subscription, nc *nats.Conn, done chan struct{}) {
	defer close(done)

	var wg sync.WaitGroup
	for i := 0; i < writeWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.drainWorker(ctx, db, cols, batchSize, rowCh)
		}()
	}

	<-ctx.Done()
	_ = sub.Unsubscribe() // stop new deliveries; buffered rows stay in rowCh
	wg.Wait()             // workers drain the remainder and flush before exiting

	nc.Close()
	db.Close()
	r.mu.Lock()
	if r.status == StatusRunning {
		r.status = StatusStopped
	}
	r.nc = nil
	r.rowCh = nil
	r.mu.Unlock()
	slog.Info("recording stopped", "id", r.spec.ID, "received", r.received.Load(),
		"written", r.written.Load(), "errors", r.errors.Load(), "dropped", r.dropped.Load())
}

// drainWorker batches rows from rowCh and writes them, flushing on batch size or
// a timer. On ctx cancellation it drains whatever remains in the buffer (shared
// cooperatively with the other workers) and does a final flush before exiting.
func (r *Recorder) drainWorker(ctx context.Context, db *sql.DB, cols []schema.ColumnDef, batchSize int, rowCh chan pg.Row) {
	ticker := time.NewTicker(writeFlushEvery)
	defer ticker.Stop()

	batch := make([]pg.Row, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := pg.InsertBatch(db, r.spec.Table, cols, batch); err != nil {
			r.errors.Add(uint64(len(batch)))
			slog.Error("batch insert failed", "id", r.spec.ID, "rows", len(batch), "err", err)
		} else {
			r.written.Add(uint64(len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for drained := false; !drained; {
				select {
				case row := <-rowCh:
					batch = append(batch, row)
					if len(batch) >= batchSize {
						flush()
					}
				default:
					drained = true
				}
			}
			flush()
			return
		case row := <-rowCh:
			batch = append(batch, row)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (r *Recorder) handler(protoTemplate proto.Message, cols []schema.ColumnDef, rowCh chan pg.Row) nats.MsgHandler {
	// One reusable decode buffer per subscription. NATS delivers to an async
	// subscription's callback from a single goroutine, sequentially, so we can
	// decode every message into the same message value instead of cloning the
	// template per message — removing a hot-path allocation (and the GC churn it
	// causes at high rates). proto.Unmarshal discards the message's existing
	// contents before decoding, so no stale fields leak between messages, and
	// schema.ExtractValues copies every value out (no references retained into
	// the buffer) before the next message reuses it.
	msgBuf := protoTemplate.ProtoReflect().New().Interface()
	return func(msg *nats.Msg) {
		n := r.received.Add(1)

		// The rule is read lock-free so it can be swapped on the fly.
		g := r.gate.Load()

		// Percentage sampling is always the first gate, so rejected messages
		// cost only this counter and comparison. Messages that pass are decoded
		// once, then every configured payload filter is ANDed below.
		if !g.keepCount(n) {
			return
		}

		if err := proto.Unmarshal(msg.Data, msgBuf); err != nil {
			slog.Debug("unmarshal error", "id", r.spec.ID, "subject", msg.Subject, "err", err)
			r.errors.Add(1)
			return
		}

		if !g.keepField(msgBuf) {
			return
		}

		vals := schema.ExtractValues(msgBuf, cols)

		// Non-blocking hand-off to the batch writer. If the buffer is full we
		// drop (and count it) rather than block NATS delivery — visible
		// backpressure beats silent latency.
		select {
		case rowCh <- pg.Row{Subject: msg.Subject, Vals: vals}:
		default:
			r.dropped.Add(1)
		}
	}
}

// Stop cancels the recording and waits for it to drain.
func (r *Recorder) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	if r.status != StatusRunning || cancel == nil {
		r.status = StatusStopped
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	cancel()
	<-done

	r.mu.Lock()
	r.status = StatusStopped
	r.cancel = nil
	r.mu.Unlock()
}

// Run records a single topic from cfg until ctx is cancelled. It preserves the
// behavior of the original one-shot `start` command on top of the Recorder type.
func Run(ctx context.Context, cfg *config.Config, resolve Resolver) error {
	// Guarded here as well as in config.Load, so no caller can start an
	// unbounded headless recording.
	if cfg.RetentionHours < 1 {
		return fmt.Errorf("retention_hours must be at least 1: a headless run may not record without a retention window")
	}

	if resolve == nil {
		return fmt.Errorf("proto catalogue resolver is required")
	}
	tmpl, err := resolve(cfg.Topic)
	if err != nil {
		return err
	}
	cols := cfg.Columns
	if len(cols) == 0 {
		cols = schema.Introspect(tmpl)
		slog.Info("auto-generated columns from proto", "count", len(cols), "topic", cfg.Topic)
	}

	rec := NewResolved(startSpec(cfg, cols), resolve, tmpl)

	preset := natspreset.Preset{
		Name:     "config",
		URL:      cfg.NATS.Address,
		Username: cfg.NATS.Username,
		Password: cfg.NATS.Password,
	}

	// Retention has to be enforceable before anything is recorded: if the
	// sweeper cannot reach Postgres, a headless run would grow without bound.
	sweepDB, err := pg.Connect(cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("retention sweeper: %w", err)
	}
	defer sweepDB.Close()

	if err := rec.Start(ctx, cfg.Postgres.DSN, preset); err != nil {
		return err
	}

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		sweepRetention(ctx, sweepDB, rec.Spec().Table, rec.Spec().RetentionHours)
	}()

	<-ctx.Done()
	rec.Stop()
	<-swept // let an in-flight delete finish before the pool is closed
	return nil
}

// startSpec builds the headless run's job definition. Retention is mandatory,
// so config.Load has already resolved a bounded window.
func startSpec(cfg *config.Config, cols []schema.ColumnDef) JobSpec {
	return JobSpec{
		ID:             "start",
		Topic:          cfg.Topic,
		Table:          cfg.Table,
		SampleRate:     cfg.SampleRate,
		NATSEnv:        "config",
		Columns:        cols,
		RetentionHours: cfg.RetentionHours,
	}
}

// sweepRetention enforces the window for a headless run, which has no manager
// to do it. Without this the `start` path would keep rows forever. The pool is
// owned by the caller; transient delete failures are logged and retried on the
// next tick rather than ending the sweep.
func sweepRetention(ctx context.Context, db *sql.DB, table string, hours int) {
	ticker := time.NewTicker(config.RetentionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := pg.DeleteOlderThan(db, table, hours)
			if err != nil {
				slog.Warn("retention cleanup failed", "table", table, "err", err)
			} else if n > 0 {
				slog.Info("retention cleanup", "table", table, "deleted", n, "older_than_hours", hours)
			}
		}
	}
}
