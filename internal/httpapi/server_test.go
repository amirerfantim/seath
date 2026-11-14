package httpapi

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"seath/internal/config"
	"seath/internal/manager"
	"seath/internal/recorder"
	"seath/internal/schema"
)

// testServer builds a server whose manager only needs to answer for limits.
func testServer() *server {
	return &server{mgr: manager.New(context.Background(), nil, "", nil, config.DefaultMaxRetentionHours, config.DefaultQueryTimeoutSeconds, config.DefaultQueryMaxRows)}
}

func TestLimitsExposeQueryTimeout(t *testing.T) {
	s := &server{mgr: manager.New(context.Background(), nil, "", nil, config.DefaultMaxRetentionHours, 123, 23456)}
	rec := httptest.NewRecorder()
	s.limits(rec, httptest.NewRequest(http.MethodGet, "/api/config", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"query_timeout_seconds":123`) {
		t.Fatalf("response does not expose query timeout: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"query_max_rows":23456`) {
		t.Fatalf("response does not expose query row limit: %s", rec.Body.String())
	}
}

func TestExportRecordingCSVRejectsUnknownRecordingBeforeDownload(t *testing.T) {
	s := testServer()
	req := httptest.NewRequest(http.MethodGet, "/api/recordings/missing/export.csv", nil)
	req.SetPathValue("id", "missing")
	rec := httptest.NewRecorder()

	s.exportRecordingCSV(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if disposition := rec.Header().Get("Content-Disposition"); disposition != "" {
		t.Fatalf("missing recording unexpectedly became a download: %q", disposition)
	}
}

func TestRetentionLabel(t *testing.T) {
	tests := []struct {
		hours int
		want  string
	}{
		{0, "unset"},
		{1, "1h"},
		{6, "6h"},
		{24, "24h"},
	}
	for _, tc := range tests {
		if got := retentionLabel(tc.hours); got != tc.want {
			t.Errorf("retentionLabel(%d) = %q, want %q", tc.hours, got, tc.want)
		}
	}
}

func TestDescribeSpecChangeRetention(t *testing.T) {
	before := recorder.JobSpec{SampleRate: 10, RetentionHours: 0}
	after := before
	after.RetentionHours = 6

	got := describeSpecChange(before, after)
	if !strings.Contains(got, "retention unset → 6h") {
		t.Errorf("describeSpecChange = %q, want it to name the retention change in hours", got)
	}
	if strings.Contains(got, "retention unset → 6d") {
		t.Errorf("describeSpecChange still reports days: %q", got)
	}
}

func TestDescribeSpecChangeNoChange(t *testing.T) {
	spec := recorder.JobSpec{Topic: "t", Table: "tbl", SampleRate: 10, RetentionHours: 3}
	if got := describeSpecChange(spec, spec); got != "no field changes" {
		t.Errorf("identical specs: got %q, want %q", got, "no field changes")
	}
}

func TestDescribeSpecChangeColumns(t *testing.T) {
	before := recorder.JobSpec{Columns: []schema.ColumnDef{
		{Field: "score", Column: "score", Type: "double precision", Include: true},
		{Field: "count", Column: "count", Type: "double precision", Include: true},
	}}

	t.Run("rename is detected", func(t *testing.T) {
		after := recorder.JobSpec{Columns: []schema.ColumnDef{
			{Field: "score", Column: "score_value", Type: "double precision", Include: true},
			{Field: "count", Column: "count", Type: "double precision", Include: true},
		}}
		if got := describeSpecChange(before, after); !strings.Contains(got, "columns score") {
			t.Errorf("rename not reported: %q", got)
		}
	})

	t.Run("include toggle is detected", func(t *testing.T) {
		after := recorder.JobSpec{Columns: []schema.ColumnDef{
			{Field: "score", Column: "score", Type: "double precision", Include: true},
			{Field: "count", Column: "count", Type: "double precision", Include: false},
		}}
		if got := describeSpecChange(before, after); !strings.Contains(got, "columns count") {
			t.Errorf("include toggle not reported: %q", got)
		}
	})

	t.Run("duplicate definition is detected", func(t *testing.T) {
		after := recorder.JobSpec{Columns: append(append([]schema.ColumnDef{}, before.Columns...), before.Columns[0])}
		if got := describeSpecChange(before, after); got == "no field changes" {
			t.Error("a duplicated column definition should not read as no change")
		}
	})
}

// The wire contract: clients send retention_hours.
func TestJobSpecJSONRetentionField(t *testing.T) {
	var spec recorder.JobSpec
	if err := json.Unmarshal([]byte(`{"retention_hours":12}`), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.RetentionHours != 12 {
		t.Errorf("retention_hours not decoded: got %d", spec.RetentionHours)
	}

	out, err := json.Marshal(recorder.JobSpec{RetentionHours: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"retention_hours":3`) {
		t.Errorf("encoded spec missing retention_hours: %s", out)
	}
	if strings.Contains(string(out), "retention_days") {
		t.Errorf("encoded spec still emits retention_days: %s", out)
	}
}

func TestSamplingJSONCarriesTypedValues(t *testing.T) {
	var sampling recorder.Sampling
	if err := json.Unmarshal([]byte(`{"sample_mode":"field_value","sample_field":"is_enabled","sample_values":["true"]}`), &sampling); err != nil {
		t.Fatal(err)
	}
	if sampling.Mode != recorder.ModeFieldValue || sampling.Field != "is_enabled" || len(sampling.Values) != 1 || sampling.Values[0] != "true" {
		t.Fatalf("decoded sampling = %+v", sampling)
	}
	out, err := json.Marshal(sampling)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"sample_values":["true"]`) {
		t.Fatalf("encoded sampling lost typed values: %s", out)
	}
	if strings.Contains(string(out), "sample_ids") {
		t.Fatalf("encoded sampling still exposes integer-only sample_ids: %s", out)
	}
}

func TestSamplingJSONCarriesCompositeFilters(t *testing.T) {
	var sampling recorder.Sampling
	body := `{"sample_rate":10,"sample_filters":[{"type":"exact","field":"is_enabled","values":["false"]},{"type":"modulo_range","field":"entity_id","lower":45,"upper":65},{"type":"float_compare","field":"score","operator":">","value":0.8}]}`
	if err := json.Unmarshal([]byte(body), &sampling); err != nil {
		t.Fatal(err)
	}
	if sampling.Rate != 10 || len(sampling.Filters) != 3 {
		t.Fatalf("decoded sampling = %+v", sampling)
	}
	if sampling.Filters[1].Lower == nil || sampling.Filters[1].Upper == nil || *sampling.Filters[1].Lower != 45 || *sampling.Filters[1].Upper != 65 {
		t.Fatalf("modulo filter lost its bounds: %+v", sampling.Filters[1])
	}
	if sampling.Filters[2].Value == nil || *sampling.Filters[2].Value != 0.8 {
		t.Fatalf("float filter lost its threshold: %+v", sampling.Filters[2])
	}
	out, err := json.Marshal(sampling)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"sample_filters"`, `"modulo_range"`, `"float_compare"`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("encoded sampling missing %s: %s", want, out)
		}
	}
}

func TestSetSamplingRejectsOversizedBodyBeforeManager(t *testing.T) {
	rec := httptest.NewRecorder()
	body := `{"sample_rate":100,"sample_filters":[{"type":"exact","field":"label","values":["` + strings.Repeat("x", maxSpecBytes) + `"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/recordings/id/sampling", strings.NewReader(body))
	req.SetPathValue("id", "id")

	testServer().setSampling(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "oversized") {
		t.Fatalf("error should name the oversized request: %s", rec.Body.String())
	}
}

// A stale client sending the old field must be refused, not silently read as
// retention_hours == 0 (which would turn a bounded retention into forever).
func TestDecodeSpecRejectsLegacyRetentionDays(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/recordings",
		strings.NewReader(`{"topic":"t","table":"tbl","sample_rate":10,"nats_env":"prod","retention_days":7}`))

	if _, ok := testServer().decodeSpec(rec, req); ok {
		t.Fatal("a body carrying retention_days must be rejected")
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "retention_hours") {
		t.Errorf("error should point at the new field, got %s", rec.Body.String())
	}
}

func TestDecodeSpecAcceptsCurrentBody(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/recordings",
		strings.NewReader(`{"topic":"t","table":"tbl","sample_rate":10,"nats_env":"prod","retention_hours":6}`))

	spec, ok := testServer().decodeSpec(rec, req)
	if !ok {
		t.Fatalf("valid body rejected: %s", rec.Body.String())
	}
	if spec.RetentionHours != 6 {
		t.Errorf("RetentionHours = %d, want 6", spec.RetentionHours)
	}
}

func TestDecodeSpecRejectsInvalidJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/recordings", strings.NewReader(`{not json`))
	if _, ok := testServer().decodeSpec(rec, req); ok {
		t.Fatal("malformed JSON must be rejected")
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// decodeMappings goes through the real mux, exercising routing too.
func decodeMappings(t *testing.T, h http.Handler) []manager.Mapping {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/topic-mappings", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/topic-mappings = %d, want 200", rec.Code)
	}
	var body struct {
		Mappings []manager.Mapping `json:"mappings"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body.Mappings
}

// The table and the recording builder must offer the same set.
func TestListMappingsMatchesSelectableTopics(t *testing.T) {
	mgr := testServer().mgr
	got := decodeMappings(t, Handler(mgr))

	if want := len(mgr.Topics()); len(got) != want {
		t.Fatalf("got %d mappings, want %d (every selectable topic)", len(got), want)
	}
	for _, m := range got {
		if m.ProtoName == "" {
			t.Fatalf("mapping %q carries no proto name", m.Topic)
		}
	}
}

func TestSetMappingRouteRejectsUnknownProtoType(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/topic-mappings/some.topic",
		strings.NewReader(`{"proto_name":"not.a.real.ProtoType"}`))
	Handler(testServer().mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT with an unlinked type = %d, want 400", rec.Code)
	}
}

func TestSetMappingRouteRejectsInvalidJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/topic-mappings/some.topic", strings.NewReader(`{`))
	Handler(testServer().mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT with malformed JSON = %d, want 400", rec.Code)
	}
}

// A revert that did not happen must not report success.
func TestDeleteMappingRouteUnknownTopicIs404(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/topic-mappings/never.mapped", nil)
	Handler(testServer().mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE of an unmapped topic = %d, want 404", rec.Code)
	}
}

// Topic names reach the browser inside data-* attributes.
func TestSetMappingRouteRejectsInjectionInTopic(t *testing.T) {
	rec := httptest.NewRecorder()
	// Percent-encoded so the request is well-formed; PathValue decodes it back
	// to the raw quote before the manager ever sees it.
	req := httptest.NewRequest(http.MethodPut, "/api/topic-mappings/x%22%20onmouseover%3D%22alert(1)",
		strings.NewReader(`{"proto_name":"example.Lock"}`))
	Handler(testServer().mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT with a quote in the topic = %d, want 400", rec.Code)
	}
}

// POST trims its topic; PUT takes it from the URL and must do the same. Both
// outcomes are 400, so assert on which check ran: padded-and-untrimmed dies on
// the subject charset, trimmed gets as far as the proto type.
func TestSetMappingRouteTrimsPathTopic(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/topic-mappings/%20good.topic%20",
		strings.NewReader(`{"proto_name":"not.a.real.ProtoType"}`))
	Handler(testServer().mgr).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not in the active catalogue") {
		t.Fatalf("padded topic was not trimmed before validation: %s", rec.Body.String())
	}
}

// Every esc() in the embedded UI must cover attribute context. Topic names,
// table names and error text are interpolated into data-* and title=, so an
// esc() that only handles angle brackets lets a stored value open an event
// handler in an admin's browser.
func TestEmbeddedUIEscapesQuotes(t *testing.T) {
	assets, err := fs.Sub(webFS, "web")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.ReadDir(assets, ".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".js") {
			continue
		}
		b, err := fs.ReadFile(assets, e.Name())
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		i := strings.Index(src, "function esc(")
		if i < 0 {
			continue
		}
		end := strings.Index(src[i:], "\n}")
		if end < 0 {
			t.Fatalf("%s: could not delimit esc()", e.Name())
		}
		body := src[i : i+end]
		for _, want := range []string{"&amp;", "&lt;", "&gt;", "&quot;", "&#39;"} {
			if !strings.Contains(body, want) {
				t.Errorf("%s: esc() does not produce %s", e.Name(), want)
			}
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no esc() found in the embedded assets")
	}
}

func TestPresetAPIAndRegistryHaveNoEnvironmentType(t *testing.T) {
	rec := httptest.NewRecorder()
	testServer().listPresets(rec, httptest.NewRequest(http.MethodGet, "/api/presets", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/presets = %d, want 200", rec.Code)
	}
	if strings.Contains(rec.Body.String(), `"builtin"`) {
		t.Fatalf("preset API still exposes built-in type: %s", rec.Body.String())
	}

	html, err := fs.ReadFile(webFS, "web/registry.html")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(html)), "<th>type</th>") {
		t.Fatal("NATS environment table still renders a type column")
	}
	js, err := fs.ReadFile(webFS, "web/registry.js")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(js), ".builtin") {
		t.Fatal("NATS environment UI still branches on built-in type")
	}
}

func TestRecordingListUsesCompactSummaryAndFullRuleInteraction(t *testing.T) {
	js, err := fs.ReadFile(webFS, "web/app.js")
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	for _, want := range []string{"r.sample_summary || samplingSummary(r)", `title="${esc(fullSampling)}"`, `data-act="rate"`, `<td class="sample-col">`} {
		if !strings.Contains(src, want) {
			t.Fatalf("recording sampling UI is missing %q", want)
		}
	}
	html, err := fs.ReadFile(webFS, "web/index.html")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(html), `<th class="sample-col">sample</th>`) {
		t.Fatal("recording table header does not share the sample column alignment")
	}
	css, err := fs.ReadFile(webFS, "web/style.css")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{".recordings .sample-col", "min-width: 168px", "justify-content: flex-start"} {
		if !strings.Contains(string(css), want) {
			t.Fatalf("sample column styling is missing %q", want)
		}
	}
}
