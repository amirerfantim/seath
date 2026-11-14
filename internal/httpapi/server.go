// Package httpapi exposes the recorder manager over HTTP and serves the
// embedded single-page UI.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"seath/internal/auth"
	"seath/internal/config"
	"seath/internal/manager"
	"seath/internal/natspreset"
	"seath/internal/pg"
	"seath/internal/prototypes"
	"seath/internal/recorder"
	"seath/internal/schema"
	"seath/internal/sysstat"
)

// Handler builds the HTTP handler: REST API under /api plus the embedded UI.
func Handler(mgr *manager.Manager) http.Handler {
	s := &server{mgr: mgr, mon: sysstat.NewMonitor()}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /api/system", s.system)

	mux.HandleFunc("GET /api/topics", s.listTopics)
	mux.HandleFunc("GET /api/topics/{topic}/schema", s.topicSchema)
	mux.HandleFunc("GET /api/presets", s.listPresets)
	mux.HandleFunc("POST /api/presets", s.addPreset)
	mux.HandleFunc("DELETE /api/presets/{name}", s.deletePreset)
	mux.HandleFunc("GET /api/stats", s.stats)
	mux.HandleFunc("POST /api/query", s.query)

	mux.HandleFunc("GET /api/proto/modules", s.protoModules)
	mux.HandleFunc("POST /api/proto/modules", s.importProtoModule)
	mux.HandleFunc("PUT /api/proto/modules/{id}/activate", s.activateProtoModule)
	mux.HandleFunc("GET /api/proto/types", s.protoTypes)
	mux.HandleFunc("GET /api/proto/types/{name}/schema", s.protoTypeSchema)
	mux.HandleFunc("GET /api/topic-mappings", s.listMappings)
	mux.HandleFunc("POST /api/topic-mappings", s.addMapping)
	mux.HandleFunc("PUT /api/topic-mappings/{topic}", s.setMapping)
	mux.HandleFunc("DELETE /api/topic-mappings/{topic}", s.deleteMapping)

	mux.HandleFunc("GET /api/audit", s.listAudit)
	mux.HandleFunc("GET /api/me", s.whoami)
	mux.HandleFunc("GET /api/config", s.limits)

	mux.HandleFunc("GET /api/taps", s.listTaps)
	mux.HandleFunc("POST /api/taps", s.startTap)
	mux.HandleFunc("DELETE /api/taps/{id}", s.stopTap)

	mux.HandleFunc("GET /api/recordings/stream", s.streamRecordings)
	mux.HandleFunc("GET /api/taps/stream", s.streamTaps)

	mux.HandleFunc("GET /api/recordings", s.listRecordings)
	mux.HandleFunc("POST /api/recordings", s.createRecording)
	mux.HandleFunc("GET /api/recordings/{id}", s.getRecording)
	mux.HandleFunc("PUT /api/recordings/{id}", s.updateRecording)
	mux.HandleFunc("DELETE /api/recordings/{id}", s.deleteRecording)
	mux.HandleFunc("POST /api/recordings/{id}/start", s.startRecording)
	mux.HandleFunc("POST /api/recordings/{id}/stop", s.stopRecording)
	mux.HandleFunc("POST /api/recordings/{id}/sampling", s.setSampling)
	// Retained so an already-loaded page (assets are no-cache, but a tab open
	// across the redeploy isn't) keeps working against the new binary.
	mux.HandleFunc("POST /api/recordings/{id}/sample-rate", s.setSampling)
	mux.HandleFunc("GET /api/recordings/{id}/preview", s.previewRecording)
	mux.HandleFunc("GET /api/recordings/{id}/insights", s.recordingInsights)
	mux.HandleFunc("GET /api/recordings/{id}/rows", s.recordingRows)
	mux.HandleFunc("GET /api/recordings/{id}/export.csv", s.exportRecordingCSV)
	mux.HandleFunc("GET /api/recordings/{id}/activity", s.recordingActivity)

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The embedded assets change on every deploy and carry no validators,
		// so tell browsers to always revalidate — otherwise a stale cached page
		// (e.g. the old login form) keeps running after a deploy.
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		fileServer.ServeHTTP(w, r)
	}))

	return mux
}

type server struct {
	mgr *manager.Manager
	mon *sysstat.Monitor
}

// system reports live resource usage: container CPU/memory/disk, Go runtime,
// the database footprint, connection-pool stats, and recording/tap counts.
func (s *server) system(w http.ResponseWriter, _ *http.Request) {
	out := map[string]any{"resources": s.mon.Sample()}

	if st, err := s.mgr.Stats(); err == nil {
		out["database"] = st
	}

	total, running := s.mgr.RecordingCounts()
	out["recordings"] = map[string]int{
		"total":   total,
		"running": running,
		"taps":    s.mgr.TapCount(),
	}

	out["recorder_totals"] = s.mgr.RecorderAggregate()
	out["pg_host"] = s.mgr.HostStats()

	ps := s.mgr.DBStats()
	out["db_pool"] = map[string]any{
		"open":       ps.OpenConnections,
		"in_use":     ps.InUse,
		"idle":       ps.Idle,
		"max_open":   ps.MaxOpenConnections,
		"wait_count": ps.WaitCount,
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) readyz(w http.ResponseWriter, _ *http.Request) {
	if err := s.mgr.Ping(); err != nil {
		http.Error(w, "database: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func (s *server) listTopics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"topics": s.mgr.Topics()})
}

func (s *server) topicSchema(w http.ResponseWriter, r *http.Request) {
	t := r.PathValue("topic")
	msg, err := s.mgr.ResolveTopic(t)
	if err != nil {
		writeErr(w, http.StatusNotFound, "unknown topic")
		return
	}
	cols := pg.NormalizeColumns(schema.Introspect(msg))
	writeJSON(w, http.StatusOK, map[string]any{
		"topic":           t,
		"suggested_table": config.SuggestedTableName(t, time.Now()),
		"columns":         cols,
		"sampling_fields": recorder.SamplingFields(msg),
	})
}

func (s *server) protoModules(w http.ResponseWriter, _ *http.Request) {
	catalogue, err := s.mgr.ProtoCatalogue()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, catalogue)
}

func (s *server) protoTypes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"types": s.mgr.ProtoTypes()})
}

func (s *server) protoTypeSchema(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	msg, err := s.mgr.NewProto(name)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	cols := pg.NormalizeColumns(schema.Introspect(msg))
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "columns": cols})
}

const maxProtoImportBytes = prototypes.MaxDescriptorSetBytes + (1 << 20)

func (s *server) importProtoModule(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxProtoImportBytes)
	if err := r.ParseMultipartForm(maxProtoImportBytes); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid or oversized multipart upload")
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}
	file, _, err := r.FormFile("descriptor_set")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "descriptor_set file is required")
		return
	}
	defer file.Close()
	descriptors, err := io.ReadAll(io.LimitReader(file, prototypes.MaxDescriptorSetBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read descriptor_set: "+err.Error())
		return
	}
	if len(descriptors) > prototypes.MaxDescriptorSetBytes {
		writeErr(w, http.StatusBadRequest, "descriptor_set exceeds 32 MiB")
		return
	}
	module, err := s.mgr.ImportProtoModule(r.FormValue("module_path"), r.FormValue("version"), descriptors, auth.UserFromContext(r.Context()))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	auth.Annotate(r.Context(), "", "stage proto module", module.Path+"@"+module.Version)
	writeJSON(w, http.StatusCreated, module)
}

func (s *server) activateProtoModule(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeErr(w, http.StatusBadRequest, "invalid module id")
		return
	}
	module, invalidMappings, err := s.mgr.ActivateProtoModule(id)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	auth.Annotate(r.Context(), "", "activate proto module", module.Path+"@"+module.Version)
	writeJSON(w, http.StatusOK, map[string]any{"module": module, "invalid_mappings": invalidMappings})
}

func (s *server) listMappings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"mappings": s.mgr.TopicMappings()})
}

func (s *server) addMapping(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Topic     string `json:"topic"`
		ProtoName string `json:"proto_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	mapping, err := s.mgr.AddTopicMapping(strings.TrimSpace(body.Topic), strings.TrimSpace(body.ProtoName))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, mapping)
}

// setMapping re-points a topic at a different proto type. Unlike addMapping it
// replaces what is already there, so any stored topic can be corrected without
// a rebuild.
func (s *server) setMapping(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ProtoName string `json:"proto_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	mapping, err := s.mgr.SetTopicMapping(strings.TrimSpace(r.PathValue("topic")), strings.TrimSpace(body.ProtoName))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapping)
}

func (s *server) deleteMapping(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.DeleteTopicMapping(r.PathValue("topic")); err != nil {
		writeManagerErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) listAudit(w http.ResponseWriter, r *http.Request) {
	if auth.RoleFromContext(r.Context()) != auth.RoleAdmin {
		writeErr(w, http.StatusForbidden, "admin only")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, err := s.mgr.ListAudit(limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

// limits exposes server-side bounds the UI has to respect.
func (s *server) limits(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"max_retention_hours":   s.mgr.MaxRetentionHours(),
		"min_retention_hours":   1,
		"query_timeout_seconds": s.mgr.QueryTimeoutSeconds(),
		"query_max_rows":        s.mgr.QueryMaxRows(),
	})
}

func (s *server) whoami(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"user": auth.UserFromContext(r.Context()),
		"role": auth.RoleFromContext(r.Context()),
	})
}

func (s *server) listTaps(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"taps": s.mgr.ListTaps()})
}

func (s *server) startTap(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Subject string `json:"subject"`
		Env     string `json:"env"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	view, err := s.mgr.StartTap(body.Subject, body.Env)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (s *server) stopTap(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.StopTap(r.PathValue("id")); err != nil {
		writeManagerErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) listPresets(w http.ResponseWriter, _ *http.Request) {
	presets := s.mgr.Presets()
	type presetView struct {
		Name    string `json:"name"`
		URL     string `json:"url"`
		HasAuth bool   `json:"has_auth"`
	}
	out := make([]presetView, 0, len(presets))
	for _, p := range presets {
		out = append(out, presetView{
			Name:    p.Name,
			URL:     p.URL,
			HasAuth: p.Username != "" || p.Token != "",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"presets": out})
}

func (s *server) addPreset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name     string `json:"name"`
		URL      string `json:"url"`
		Username string `json:"username"`
		Password string `json:"password"`
		Token    string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	preset := natspreset.Preset{
		Name:     strings.TrimSpace(body.Name),
		URL:      strings.TrimSpace(body.URL),
		Username: body.Username,
		Password: body.Password,
		Token:    body.Token,
	}
	var err error
	if r.Header.Get("If-None-Match") == "*" {
		err = s.mgr.CreatePreset(preset)
	} else {
		err = s.mgr.AddPreset(preset)
	}
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *server) deletePreset(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.DeletePreset(r.PathValue("name")); err != nil {
		writeManagerErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) stats(w http.ResponseWriter, _ *http.Request) {
	st, err := s.mgr.Stats()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) listRecordings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.recordingsPayload())
}

func (s *server) recordingsPayload() map[string]any {
	views := s.mgr.List()
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
	return map[string]any{"recordings": views}
}

func (s *server) streamRecordings(w http.ResponseWriter, r *http.Request) {
	stream(w, r, func() any { return s.recordingsPayload() })
}

func (s *server) streamTaps(w http.ResponseWriter, r *http.Request) {
	stream(w, r, func() any { return map[string]any{"taps": s.mgr.ListTaps()} })
}

// stream pushes a JSON snapshot as a Server-Sent Event once per second until the
// client disconnects.
func stream(w http.ResponseWriter, r *http.Request, snapshot func() any) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	send := func() {
		data, err := json.Marshal(snapshot())
		if err != nil {
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
	send()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func (s *server) createRecording(w http.ResponseWriter, r *http.Request) {
	spec, ok := s.decodeSpec(w, r)
	if !ok {
		return
	}
	view, err := s.mgr.Create(spec, auth.UserFromContext(r.Context()))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	auth.Annotate(r.Context(), view.ID, actionCreated, fmt.Sprintf(
		"topic %s → table %s · sampling %s · env %s", view.Topic, view.Table, view.SampleDesc, view.NATSEnv))
	writeJSON(w, http.StatusCreated, view)
}

func (s *server) getRecording(w http.ResponseWriter, r *http.Request) {
	view, err := s.mgr.Get(r.PathValue("id"))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *server) updateRecording(w http.ResponseWriter, r *http.Request) {
	spec, ok := s.decodeSpec(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	auth.Annotate(r.Context(), id, actionEdited, "")
	view, prev, err := s.mgr.Update(id, spec)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	auth.Annotate(r.Context(), id, actionEdited, describeSpecChange(prev, view.JobSpec))
	writeJSON(w, http.StatusOK, view)
}

func (s *server) deleteRecording(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	auth.Annotate(r.Context(), id, actionDeleted, "")
	table, err := s.mgr.Delete(id)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	// Named by the delete itself, so a failed one cannot claim a drop and a
	// concurrent rename cannot make a successful one report a stale table.
	auth.Annotate(r.Context(), id, actionDeleted, "dropped table "+table)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) startRecording(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	auth.Annotate(r.Context(), id, actionStarted, "")
	view, err := s.mgr.Start(id)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *server) stopRecording(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	auth.Annotate(r.Context(), id, actionStopped, "")
	view, err := s.mgr.Stop(id)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// setSampling changes a recording's percentage and AND-composed payload
// filters while it runs. A body with only sample_rate remains the pre-existing
// percentage request; legacy exclusive-mode bodies are normalized safely.
func (s *server) setSampling(w http.ResponseWriter, r *http.Request) {
	var body recorder.Sampling
	id := r.PathValue("id")
	auth.Annotate(r.Context(), id, actionSampleRate, "")
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSpecBytes)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid, oversized, or unreadable JSON body")
		return
	}
	if body.Mode == "" {
		body.Mode = recorder.ModeRate
	}
	view, err := s.mgr.SetSampling(id, body)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	auth.Annotate(r.Context(), id, actionSampleRate, view.SampleDesc)
	writeJSON(w, http.StatusOK, view)
}

// Action names recorded in the audit trail of a recording.
const (
	actionCreated    = "created"
	actionEdited     = "edited"
	actionDeleted    = "deleted"
	actionStarted    = "started"
	actionStopped    = "stopped"
	actionSampleRate = "sample rate"
)

// describeSpecChange summarises what an edit changed, for the activity log.
func describeSpecChange(before, after recorder.JobSpec) string {
	var changes []string
	add := func(field, from, to string) {
		if from != to {
			changes = append(changes, fmt.Sprintf("%s %s → %s", field, from, to))
		}
	}
	add("topic", before.Topic, after.Topic)
	add("table", before.Table, after.Table)
	add("env", before.NATSEnv, after.NATSEnv)
	add("sample", strconv.Itoa(before.SampleRate)+"%", strconv.Itoa(after.SampleRate)+"%")
	add("retention", retentionLabel(before.RetentionHours), retentionLabel(after.RetentionHours))
	add("time index", strconv.FormatBool(before.IndexTime), strconv.FormatBool(after.IndexTime))
	if c := describeColumnChange(before.Columns, after.Columns); c != "" {
		changes = append(changes, c)
	}
	if len(changes) == 0 {
		return "no field changes"
	}
	return strings.Join(changes, " · ")
}

// describeColumnChange names the proto fields whose column definition changed,
// so a rename or an include/index toggle is not reported as an identical set.
func describeColumnChange(before, after []schema.ColumnDef) string {
	// Counted as a multiset: definitions are not required to be unique per
	// proto field, so a map keyed by field would hide a duplicated column.
	count := func(cols []schema.ColumnDef) map[schema.ColumnDef]int {
		m := make(map[schema.ColumnDef]int, len(cols))
		for _, c := range cols {
			m[c]++
		}
		return m
	}
	prev, next := count(before), count(after)
	seen := make(map[string]bool)
	var touched []string
	mark := func(c schema.ColumnDef) {
		if !seen[c.Field] {
			seen[c.Field] = true
			touched = append(touched, c.Field)
		}
	}
	for c, n := range prev {
		if next[c] != n {
			mark(c)
		}
	}
	for c, n := range next {
		if prev[c] != n {
			mark(c)
		}
	}
	if len(touched) == 0 {
		return ""
	}
	sort.Strings(touched)
	if len(touched) > 3 {
		return fmt.Sprintf("columns %s +%d more", strings.Join(touched[:3], ", "), len(touched)-3)
	}
	return "columns " + strings.Join(touched, ", ")
}

func retentionLabel(hours int) string {
	if hours <= 0 {
		return "unset"
	}
	return strconv.Itoa(hours) + "h"
}

func (s *server) recordingActivity(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, err := s.mgr.RecordingAudit(r.PathValue("id"), limit)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *server) previewRecording(w http.ResponseWriter, r *http.Request) {
	rows, err := s.mgr.Preview(r.PathValue("id"), 20)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (s *server) query(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(body.SQL) == "" {
		writeErr(w, http.StatusBadRequest, "sql is required")
		return
	}
	cols, rows, truncated, err := s.mgr.Query(r.Context(), body.SQL)
	if err != nil {
		// Query/syntax/permission errors are the user's to fix → 400 with the
		// Postgres message.
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"columns":   cols,
		"rows":      rows,
		"truncated": truncated,
		"row_count": len(rows),
	})
}

func (s *server) exportRecordingCSV(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	recording, err := s.mgr.Get(id)
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": recording.Table + ".csv"}))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	written := &countingWriter{Writer: w}
	if err := s.mgr.ExportCSV(r.Context(), id, written); err != nil {
		if written.n == 0 {
			w.Header().Del("Content-Disposition")
			writeManagerErr(w, err)
			return
		}
		slog.Error("stream recording export", "recording", id, "err", err)
	}
}

type countingWriter struct {
	io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.n += int64(n)
	return n, err
}

func (s *server) recordingInsights(w http.ResponseWriter, r *http.Request) {
	in, err := s.mgr.Insights(r.PathValue("id"))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (s *server) recordingRows(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	cols, rows, err := s.mgr.Rows(r.PathValue("id"), limit, offset, q.Get("order"))
	if err != nil {
		writeManagerErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"columns": cols, "rows": rows})
}

func (s *server) decodeSpec(w http.ResponseWriter, r *http.Request) (recorder.JobSpec, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSpecBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "request body too large or unreadable")
		return recorder.JobSpec{}, false
	}
	// A client still sending retention_days would otherwise be read as
	// retention_hours == 0, silently turning a bounded retention into "keep
	// forever". Key presence is what matters — the value may be any JSON.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return recorder.JobSpec{}, false
	}
	if _, ok := fields["retention_days"]; ok {
		writeErr(w, http.StatusBadRequest,
			fmt.Sprintf("retention_days is no longer accepted — send retention_hours (1-%d). Reload the page to get the current UI.", s.mgr.MaxRetentionHours()))
		return recorder.JobSpec{}, false
	}
	var spec recorder.JobSpec
	if err := json.Unmarshal(body, &spec); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return recorder.JobSpec{}, false
	}
	return spec, true
}

const maxSpecBytes = 1 << 20

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeManagerErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, manager.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, manager.ErrConflict):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, manager.ErrDuplicate):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, manager.ErrMappingExists):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, manager.ErrMapping):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, manager.ErrProto):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, manager.ErrInUse):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, manager.ErrValidate):
		writeErr(w, http.StatusBadRequest, err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}
