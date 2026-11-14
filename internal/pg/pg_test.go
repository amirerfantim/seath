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
