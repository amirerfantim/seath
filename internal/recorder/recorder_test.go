package recorder

import (
	"context"
	"strings"
	"testing"

	"seath/internal/config"
	"seath/internal/schema"
)

// The headless `start` path has no manager to enforce retention, so its spec
// must carry the window resolved by config.
func TestStartSpecCarriesRetention(t *testing.T) {
	cfg := &config.Config{
		Topic:             "some.topic",
		Table:             "some_table",
		SampleRate:        10,
		MaxRetentionHours: 24,
		RetentionHours:    6,
	}
	spec := startSpec(cfg, []schema.ColumnDef{{Field: "a", Column: "a", Type: "text", Include: true}})

	if spec.RetentionHours != 6 {
		t.Errorf("RetentionHours = %d, want 6 — a headless run must not record without a window", spec.RetentionHours)
	}
	if spec.Table != "some_table" || spec.Topic != "some.topic" || spec.SampleRate != 10 {
		t.Errorf("spec did not carry the config through: %+v", spec)
	}
}

func TestStartSpecNeverUnbounded(t *testing.T) {
	// config.Load resolves an unset window to the ceiling; if that ever
	// regresses, the spec would silently record forever.
	cfg := &config.Config{Topic: "t", Table: "tbl", SampleRate: 1, MaxRetentionHours: 24, RetentionHours: 24}
	if spec := startSpec(cfg, nil); spec.RetentionHours < 1 {
		t.Errorf("RetentionHours = %d, want a bounded window", spec.RetentionHours)
	}
}

// Run must refuse an unbounded headless recording before it starts anything,
// even if a caller bypasses config.Load.
func TestRunRefusesWithoutRetention(t *testing.T) {
	for _, hours := range []int{0, -1} {
		cfg := &config.Config{
			Topic: "some.topic", Table: "tbl", SampleRate: 10,
			MaxRetentionHours: 24, RetentionHours: hours,
		}
		err := Run(context.Background(), cfg, nil)
		if err == nil {
			t.Fatalf("retention %d: Run should refuse to start", hours)
		}
		if !strings.Contains(err.Error(), "retention") {
			t.Errorf("retention %d: error should explain the window, got %v", hours, err)
		}
	}
}
