package manager

import (
	"strings"
	"testing"
)

// Both columns must keep being created: retention_days stays as the rollback
// shim, so a pre-v1.1 image never reads a bounded retention as "keep forever".
func TestAlterAddCountersKeepsBothRetentionColumns(t *testing.T) {
	for _, col := range []string{"retention_hours", "retention_days"} {
		if !strings.Contains(alterAddCounters, col) {
			t.Errorf("alterAddCounters must add %s", col)
		}
	}
}

func TestSamplingSQLPersistsCompositeFiltersAndLegacyValues(t *testing.T) {
	for name, stmt := range map[string]string{
		"migration": alterAddCounters,
		"load":      selectJobsSQL,
		"save":      upsertJobSQL,
	} {
		if !strings.Contains(stmt, "sample_values") {
			t.Errorf("%s does not persist sample_values", name)
		}
		if !strings.Contains(stmt, "sample_filters") {
			t.Errorf("%s does not persist sample_filters", name)
		}
		if strings.Contains(stmt, "sample_ids") {
			t.Errorf("%s still uses the integer-only sample_ids column", name)
		}
	}
}

func TestRecordingProtoSnapshotIsAdditiveAndRoundTrips(t *testing.T) {
	for name, stmt := range map[string]string{
		"migration": alterAddCounters,
		"load":      selectJobsSQL,
		"save":      upsertJobSQL,
	} {
		for _, column := range []string{"proto_name", "proto_module_ids"} {
			if !strings.Contains(stmt, column) {
				t.Errorf("%s does not persist %s", name, column)
			}
		}
	}
	update := upsertJobSQL[strings.Index(upsertJobSQL, "DO UPDATE SET"):]
	for _, column := range []string{"proto_name", "proto_module_ids"} {
		if !strings.Contains(update, column+" = EXCLUDED."+column) {
			t.Errorf("recording edits do not preserve %s", column)
		}
	}
}

const testMax = 24

func TestReconcileRetention(t *testing.T) {
	tests := []struct {
		name          string
		days, hours   int
		maxHours      int
		wantHours     int
		wantShortened bool
	}{
		{"never configured becomes the ceiling", 0, 0, testMax, 24, true},
		{"legacy one day", 1, 0, testMax, 24, false},
		{"legacy week is clamped", 7, 0, testMax, 24, true},
		{"legacy month is clamped", 30, 0, testMax, 24, true},
		{"huge legacy value does not overflow", 1 << 30, 0, testMax, 24, true},
		{"already migrated row is left alone", 1, 6, testMax, 6, false},
		{"already migrated at the ceiling", 1, 24, testMax, 24, false},
		// A rolled-back image writing days only, on top of a v1.1 hours value.
		{"rollback multi-day edit wins over stale hours", 7, 1, testMax, 24, true},
		{"rollback two-day edit wins over stale hours", 2, 6, testMax, 24, true},
		{"rollback keep-forever is not honoured", 0, 6, testMax, 24, true},
		{"rollback keep-forever at ceiling", 0, 24, testMax, 24, true},
		// A lowered ceiling must pull existing windows down with it.
		{"lowered ceiling clamps a stored window", 1, 24, 6, 6, true},
		{"lowered ceiling leaves a smaller window", 1, 2, 6, 2, false},
		{"raised ceiling does not inflate a window", 1, 6, 48, 6, false},
		{"raised ceiling adopts a legacy week", 7, 0, 48, 48, true},
		{"invalid ceiling falls back to the default", 1, 0, 0, 24, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotHours, gotDays, gotShortened := reconcileRetention(tc.days, tc.hours, tc.maxHours)
			if gotHours != tc.wantHours {
				t.Errorf("reconcileRetention(%d, %d, max %d) hours = %d, want %d",
					tc.days, tc.hours, tc.maxHours, gotHours, tc.wantHours)
			}
			if gotShortened != tc.wantShortened {
				t.Errorf("reconcileRetention(%d, %d, max %d) shortened = %v, want %v",
					tc.days, tc.hours, tc.maxHours, gotShortened, tc.wantShortened)
			}
			if gotDays != 1 {
				t.Errorf("every reconciled row must carry the shim day, got %d", gotDays)
			}
		})
	}
}

// Retention is mandatory: no input may produce an unbounded or zero window.
func TestReconcileRetentionIsAlwaysBounded(t *testing.T) {
	for _, maxHours := range []int{1, 6, 24, 72} {
		for days := 0; days <= 40; days++ {
			for hours := 0; hours <= 30; hours++ {
				h, d, _ := reconcileRetention(days, hours, maxHours)
				if h < 1 {
					t.Fatalf("reconcileRetention(%d, %d, %d) = %dh — retention must never be unset", days, hours, maxHours, h)
				}
				if h > maxHours {
					t.Fatalf("reconcileRetention(%d, %d, %d) = %dh — over the ceiling", days, hours, maxHours, h)
				}
				if d != 1 {
					t.Fatalf("reconcileRetention(%d, %d, %d) shim = %d, want 1", days, hours, maxHours, d)
				}
			}
		}
	}
}

// Running it twice must change nothing the second time, or every boot would
// rewrite the same rows.
func TestReconcileRetentionIsIdempotent(t *testing.T) {
	for _, maxHours := range []int{1, 6, 24, 72} {
		for days := 0; days <= 40; days++ {
			for hours := 0; hours <= 30; hours++ {
				h1, d1, _ := reconcileRetention(days, hours, maxHours)
				h2, d2, shortened := reconcileRetention(d1, h1, maxHours)
				if h2 != h1 || d2 != d1 {
					t.Fatalf("second pass over (%d, %d, max %d) moved (%d, %d) → (%d, %d)",
						days, hours, maxHours, h1, d1, h2, d2)
				}
				if shortened {
					t.Fatalf("second pass over (%d, %d, max %d) reported another shortening", days, hours, maxHours)
				}
			}
		}
	}
}

// The shim must never report "keep forever" for a recording that has a window.
func TestRetentionDaysShim(t *testing.T) {
	tests := []struct {
		hours int
		want  int
	}{
		{0, 0},
		{1, 1},
		{12, 1},
		{24, 1},
	}
	for _, tc := range tests {
		if got := retentionDaysShim(tc.hours); got != tc.want {
			t.Errorf("retentionDaysShim(%d) = %d, want %d", tc.hours, got, tc.want)
		}
	}
	if retentionDaysShim(1) == 0 {
		t.Error("a 1-hour retention must not degrade to keep-forever on rollback")
	}
}

// Reads and writes must agree on the column, or retention silently resets.
func TestJobsSQLUsesRetentionHours(t *testing.T) {
	for name, stmt := range map[string]string{
		"selectJobsSQL": selectJobsSQL,
		"upsertJobSQL":  upsertJobSQL,
	} {
		if !strings.Contains(stmt, "retention_hours") {
			t.Errorf("%s does not reference retention_hours", name)
		}
	}
	// The shim has to be synced on edits too, not only on insert — otherwise a
	// recording edited under v1.1 keeps a stale day value for any rollback.
	update := upsertJobSQL[strings.Index(upsertJobSQL, "DO UPDATE SET"):]
	if !strings.Contains(update, "retention_days = EXCLUDED.retention_days") {
		t.Error("the conflict branch must re-sync retention_days, or an edit leaves the rollback shim stale")
	}
	if strings.Contains(selectJobsSQL, "retention_days") {
		t.Error("selectJobsSQL must read retention_hours, not the shim")
	}
}

// The upsert must keep ownership insert-only.
func TestUpsertDoesNotReassignOwnership(t *testing.T) {
	update := upsertJobSQL[strings.Index(upsertJobSQL, "DO UPDATE SET"):]
	for _, col := range []string{"created_by", "created_at"} {
		if strings.Contains(update, col+" = EXCLUDED") {
			t.Errorf("%s must not be in the DO UPDATE SET list — an edit would reassign it", col)
		}
	}
	if !strings.Contains(upsertJobSQL, "created_by") {
		t.Error("created_by must still be inserted")
	}
}
