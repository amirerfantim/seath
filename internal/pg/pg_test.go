package pg

import (
	"strings"
	"testing"
	"time"
)

// The retention cutoff must be a bound parameter and the unit must be hours —
// a stray "days" here silently keeps 24x more data than configured.
func TestDeleteOlderThanSQL(t *testing.T) {
	got := deleteOlderThanSQL("example_events_raw")
	want := `DELETE FROM "example_events_raw" WHERE received_at < NOW() - ($1 || ' hours')::interval`
	if got != want {
		t.Errorf("deleteOlderThanSQL:\n got %q\nwant %q", got, want)
	}
	if strings.Contains(got, "days") {
		t.Errorf("retention delete still uses days: %q", got)
	}
}

func TestTableExportSQLQuotesIdentifier(t *testing.T) {
	got := tableExportSQL(`rows"; DROP TABLE recorder_users; --`)
	want := `SELECT * FROM "rows""; DROP TABLE recorder_users; --" ORDER BY id ASC`
	if got != want {
		t.Fatalf("tableExportSQL() = %q, want %q", got, want)
	}
}

func TestCSVValue(t *testing.T) {
	when := time.Date(2026, time.September, 11, 1, 2, 3, 456, time.FixedZone("TEST", 5*60*60+30*60))
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"null", nil, ""},
		{"bytes", []byte(`{"ok":true}`), `{"ok":true}`},
		{"timestamp", when, "2026-09-11T01:02:03.000000456+05:30"},
		{"number", int64(42), "42"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := csvValue(tc.value); got != tc.want {
				t.Errorf("csvValue() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeleteOlderThanSQLQuotesIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  string
	}{
		{"plain", "rows", `"rows"`},
		{"reserved word", "select", `"select"`},
		{"embedded quote", `ta"ble`, `"ta""ble"`},
		{"injection attempt", `x"; DROP TABLE users; --`, `"x""; DROP TABLE users; --"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deleteOlderThanSQL(tc.table)
			if !strings.Contains(got, tc.want) {
				t.Errorf("table %q: want quoted %s in %q", tc.table, tc.want, got)
			}
		})
	}
}

func TestGrantSelectSQLQuotesIdentifiers(t *testing.T) {
	got := GrantSelectSQL(`events"; DROP TABLE x; --`, "seath_reader")
	want := `GRANT SELECT ON "events""; DROP TABLE x; --" TO "seath_reader"`
	if got != want {
		t.Errorf("GrantSelectSQL:\n got %q\nwant %q", got, want)
	}
}

func TestReaderProblems(t *testing.T) {
	if p := readerProblems("seath_reader", "seath", false, false, false, nil, nil, nil); len(p) != 0 {
		t.Fatalf("a clean reader reported problems: %v", p)
	}
	for _, tc := range []struct {
		name string
		got  []string
		want string
	}{
		{"same role", readerProblems("seath", "seath", false, false, false, nil, nil, nil), "same role as PG_DSN"},
		{"superuser", readerProblems("r", "seath", true, false, false, nil, nil, nil), "superuser"},
		{"bypassrls", readerProblems("r", "seath", false, true, false, nil, nil, nil), "BYPASSRLS"},
		{"createrole", readerProblems("r", "seath", false, false, true, nil, nil, nil), "CREATEROLE"},
		{"member of main", readerProblems("r", "seath", false, false, false, []string{"seath"}, nil, nil), "member of seath"},
		{"member of any role", readerProblems("r", "seath", false, false, false, []string{"ops"}, nil, nil), "must not belong to any role"},
		{"built-in role", readerProblems("r", "seath", false, false, false, []string{"pg_signal_backend"}, nil, nil), "pg_signal_backend"},
		{"protected table", readerProblems("r", "seath", false, false, false, nil, []string{"recorder_presets"}, nil), "recorder_presets"},
		{"missing table", readerProblems("r", "seath", false, false, false, nil, nil, []string{"recorder_users"}), "not found"},
	} {
		if len(tc.got) != 1 || !strings.Contains(tc.got[0], tc.want) {
			t.Errorf("%s: problems = %v, want one containing %q", tc.name, tc.got, tc.want)
		}
	}
}

// The reader must be refused access to every table that holds secrets or
// application state, not only the presets.
func TestProtectedTablesCoverSecrets(t *testing.T) {
	have := map[string]bool{}
	for _, t := range ProtectedTables {
		have[t] = true
	}
	for _, want := range []string{"recorder_presets", "recorder_users", "recorder_audit", "recorder_jobs"} {
		if !have[want] {
			t.Errorf("ProtectedTables is missing %s", want)
		}
	}
}

func TestIdentQuotesAndFoldsLikeUnquotedSQL(t *testing.T) {
	for in, want := range map[string]string{"events": `"events"`, "userId": `"userid"`, "_x9": `"_x9"`} {
		if got, err := ident(in); err != nil || got != want {
			t.Errorf("ident(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "a b", `a"b`, "x); DROP TABLE y; --", "9a", strings.Repeat("a", 64)} {
		if _, err := ident(bad); err == nil {
			t.Errorf("ident(%q) accepted an unsafe name", bad)
		}
	}
}

func TestProtectedTableNames(t *testing.T) {
	if !IsProtectedTable("recorder_presets") || !IsProtectedTable("RECORDER_USERS") || IsProtectedTable("events") {
		t.Error("IsProtectedTable gives the wrong answer")
	}
	if ValidTableName("Events") || !ValidTableName("events_1") {
		t.Error("ValidTableName must accept only lower-case identifiers")
	}
}
