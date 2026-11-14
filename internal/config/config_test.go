package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSuggestedTableNameIncludesUTCDate(t *testing.T) {
	now := time.Date(2026, time.August, 25, 23, 45, 0, 0, time.FixedZone("TEST", 5*60*60+30*60))
	if got, want := SuggestedTableName("example.events/order-created", now), "example_events_order_created_20260825"; got != want {
		t.Fatalf("SuggestedTableName() = %q, want %q", got, want)
	}
}

func TestSuggestedTableNameKeepsDateWithinPostgresLimit(t *testing.T) {
	now := time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC)
	got := SuggestedTableName("a."+strings.Repeat("long-topic-", 10), now)
	if len(got) != 63 {
		t.Fatalf("identifier length = %d, want PostgreSQL maximum 63: %q", len(got), got)
	}
	if !strings.HasSuffix(got, "_20260825") {
		t.Fatalf("date suffix was truncated: %q", got)
	}
}

func TestMaxRetentionHoursDefault(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")

	cfg, err := LoadBase()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRetentionHours != DefaultMaxRetentionHours {
		t.Errorf("MaxRetentionHours = %d, want the %d default", cfg.MaxRetentionHours, DefaultMaxRetentionHours)
	}
}

func TestMaxRetentionHoursEnvOverride(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv("MAX_RETENTION_HOURS", "72")

	cfg, err := LoadBase()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRetentionHours != 72 {
		t.Errorf("MaxRetentionHours = %d, want 72 from the environment", cfg.MaxRetentionHours)
	}
}

func TestQueryTimeoutDefaultAndEnvOverride(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")

	cfg, err := LoadBase()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueryTimeoutSeconds != DefaultQueryTimeoutSeconds {
		t.Errorf("QueryTimeoutSeconds = %d, want the %d default", cfg.QueryTimeoutSeconds, DefaultQueryTimeoutSeconds)
	}

	t.Setenv("QUERY_TIMEOUT_SECONDS", "123")
	cfg, err = LoadBase()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueryTimeoutSeconds != 123 {
		t.Errorf("QueryTimeoutSeconds = %d, want 123 from the environment", cfg.QueryTimeoutSeconds)
	}
}

func TestQueryTimeoutRejectsUnusableValues(t *testing.T) {
	for _, given := range []string{"0", "-1"} {
		t.Run(given, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", "")
			t.Setenv("QUERY_TIMEOUT_SECONDS", given)

			if _, err := LoadBase(); err == nil {
				t.Errorf("QUERY_TIMEOUT_SECONDS=%s should be refused", given)
			}
		})
	}
}

func TestQueryMaxRowsDefaultAndEnvOverride(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")

	cfg, err := LoadBase()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueryMaxRows != DefaultQueryMaxRows {
		t.Errorf("QueryMaxRows = %d, want the %d default", cfg.QueryMaxRows, DefaultQueryMaxRows)
	}

	t.Setenv("QUERY_MAX_ROWS", "25000")
	cfg, err = LoadBase()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueryMaxRows != 25000 {
		t.Errorf("QueryMaxRows = %d, want 25000 from the environment", cfg.QueryMaxRows)
	}
}

func TestQueryMaxRowsRejectsUnusableValues(t *testing.T) {
	for _, given := range []string{"0", "-1"} {
		t.Run(given, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", "")
			t.Setenv("QUERY_MAX_ROWS", given)

			if _, err := LoadBase(); err == nil {
				t.Errorf("QUERY_MAX_ROWS=%s should be refused", given)
			}
		})
	}
}

// A ceiling below one hour would mean no legal retention value at all, so it is
// refused at boot rather than silently ignored.
func TestMaxRetentionHoursRejectsUnusableValues(t *testing.T) {
	for _, given := range []string{"0", "-1"} {
		t.Run(given, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", "")
			t.Setenv("MAX_RETENTION_HOURS", given)

			if _, err := LoadBase(); err == nil {
				t.Errorf("MAX_RETENTION_HOURS=%s should be refused", given)
			}
		})
	}
}

func TestNumericEnvironmentRejectsInvalidValues(t *testing.T) {
	for _, key := range []string{"SAMPLE_RATE", "RETENTION_HOURS", "MAX_RETENTION_HOURS", "QUERY_TIMEOUT_SECONDS", "QUERY_MAX_ROWS"} {
		for _, value := range []string{"", "not-a-number"} {
			t.Run(key+value, func(t *testing.T) {
				t.Setenv("CONFIG_PATH", "")
				t.Setenv(key, value)
				if _, err := LoadBase(); err == nil {
					t.Fatal("invalid environment accepted")
				}
			})
		}
	}
}

func TestExplicitConfigAndEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	t.Setenv("CONFIG_PATH", path)
	if _, err := LoadBase(); err == nil {
		t.Fatal("missing explicit config accepted")
	}
	if err := os.WriteFile(path, []byte("query_max_rows: 42\nmax_retention_hours: 12\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadBase()
	if err != nil || cfg.QueryMaxRows != 42 || cfg.MaxRetentionHours != 12 {
		t.Fatalf("file not loaded: %+v %v", cfg, err)
	}
	t.Setenv("QUERY_MAX_ROWS", "73")
	cfg, err = LoadBase()
	if err != nil || cfg.QueryMaxRows != 73 {
		t.Fatalf("override not loaded: %+v %v", cfg, err)
	}
	if err := os.WriteFile(path, []byte("query_max_rows: [broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBase(); err == nil {
		t.Fatal("malformed config accepted")
	}
}

// The headless `start` command must never run without a retention window.
func TestLoadResolvesRetentionForStart(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv("TOPIC", "some.topic")
	t.Setenv("SAMPLE_RATE", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RetentionHours != cfg.MaxRetentionHours {
		t.Errorf("RetentionHours = %d, want the %d ceiling when unset", cfg.RetentionHours, cfg.MaxRetentionHours)
	}
	if cfg.RetentionHours < 1 {
		t.Error("a headless run must never be left unbounded")
	}
}

func TestLoadRetentionOverrideAndBounds(t *testing.T) {
	t.Run("override is honoured", func(t *testing.T) {
		t.Setenv("CONFIG_PATH", "")
		t.Setenv("TOPIC", "some.topic")
		t.Setenv("RETENTION_HOURS", "6")

		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.RetentionHours != 6 {
			t.Errorf("RetentionHours = %d, want 6", cfg.RetentionHours)
		}
	})

	for _, given := range []string{"25", "-1"} {
		t.Run("rejects "+given, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", "")
			t.Setenv("TOPIC", "some.topic")
			t.Setenv("RETENTION_HOURS", given)

			if _, err := Load(); err == nil {
				t.Errorf("RETENTION_HOURS=%s should be refused", given)
			}
		})
	}
}
