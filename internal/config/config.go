package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"seath/internal/schema"
)

type NATSConfig struct {
	Address  string `yaml:"address"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Mode     string `yaml:"mode"` // core | jetstream
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// DefaultMaxRetentionHours is the ceiling on a recording's retention window
// when nothing overrides it. Recordings are short-lived diagnostic captures, so
// the default is a day.
const DefaultMaxRetentionHours = 24

// DefaultQueryTimeoutSeconds bounds interactive SQL and full-table exports.
// It is intentionally generous enough for diagnostic queries while still
// preventing abandoned requests from occupying a database connection forever.
const DefaultQueryTimeoutSeconds = 90

// DefaultQueryMaxRows is the maximum number of rows returned by the interactive
// SQL endpoint. Full-table CSV exports deliberately do not use this limit.
const DefaultQueryMaxRows = 15000

// RetentionSweepInterval is how often retention is enforced. It has to stay
// well under an hour, since retention is expressed in hours and the smallest
// window is one.
const RetentionSweepInterval = 5 * time.Minute

type Config struct {
	NATS       NATSConfig         `yaml:"nats"`
	Postgres   PostgresConfig     `yaml:"postgres"`
	Topic      string             `yaml:"topic"`
	Table      string             `yaml:"table"`
	SampleRate int                `yaml:"sample_rate"` // percentage 1-100; 100=all, 10=10%, 1=1%
	Columns    []schema.ColumnDef `yaml:"columns"`

	// MaxRetentionHours caps every recording's retention window. Retention is
	// mandatory, so the effective range is 1..MaxRetentionHours; there is no
	// "keep forever".
	MaxRetentionHours int `yaml:"max_retention_hours"`

	// QueryTimeoutSeconds bounds read-only SQL queries and full-table exports.
	QueryTimeoutSeconds int `yaml:"query_timeout_seconds"`
	// QueryMaxRows caps interactive SQL responses. It does not cap CSV exports.
	QueryMaxRows int `yaml:"query_max_rows"`

	// RetentionHours is the window used by the headless `start` command. Unset
	// means MaxRetentionHours — never unlimited.
	RetentionHours int `yaml:"retention_hours"`
}

// LoadBase reads the config file and applies env overrides, without the
// start-specific validation. Used by `serve`, which only needs the DSN.
func LoadBase() (*Config, error) {
	cfg := defaults()

	path := "/etc/seath/config.yaml"
	if p := os.Getenv("CONFIG_PATH"); p != "" {
		path = p
	}
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	} else if os.Getenv("CONFIG_PATH") != "" || !os.IsNotExist(err) {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	if err := applyEnv(cfg); err != nil {
		return nil, err
	}
	if cfg.MaxRetentionHours < 1 {
		return nil, fmt.Errorf("max_retention_hours must be >= 1 (MAX_RETENTION_HOURS env or config.yaml), got %d — retention is mandatory, there is no keep-forever", cfg.MaxRetentionHours)
	}
	if cfg.QueryTimeoutSeconds < 1 {
		return nil, fmt.Errorf("query_timeout_seconds must be >= 1 (QUERY_TIMEOUT_SECONDS env or config.yaml), got %d", cfg.QueryTimeoutSeconds)
	}
	if cfg.QueryMaxRows < 1 {
		return nil, fmt.Errorf("query_max_rows must be >= 1 (QUERY_MAX_ROWS env or config.yaml), got %d", cfg.QueryMaxRows)
	}
	return cfg, nil
}

// Load reads config and validates it for a single-topic `start` run.
func Load() (*Config, error) {
	cfg, err := LoadBase()
	if err != nil {
		return nil, err
	}

	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is required (TOPIC env or config.yaml topic field)")
	}
	if cfg.Table == "" {
		cfg.Table = SuggestedTableName(cfg.Topic, time.Now())
	}
	if cfg.SampleRate < 1 || cfg.SampleRate > 100 {
		return nil, fmt.Errorf("sample_rate must be 1-100, got %d", cfg.SampleRate)
	}
	// Retention is mandatory here too: an unset window takes the ceiling rather
	// than recording forever.
	if cfg.RetentionHours == 0 {
		cfg.RetentionHours = cfg.MaxRetentionHours
	}
	if cfg.RetentionHours < 1 || cfg.RetentionHours > cfg.MaxRetentionHours {
		return nil, fmt.Errorf("retention_hours must be 1-%d, got %d", cfg.MaxRetentionHours, cfg.RetentionHours)
	}

	return cfg, nil
}

func defaults() *Config {
	return &Config{
		NATS: NATSConfig{
			Address: "nats://127.0.0.1:4222",
			Mode:    "core",
		},
		SampleRate:          100,
		MaxRetentionHours:   DefaultMaxRetentionHours,
		QueryTimeoutSeconds: DefaultQueryTimeoutSeconds,
		QueryMaxRows:        DefaultQueryMaxRows,
	}
}

func applyEnv(cfg *Config) error {
	if v := os.Getenv("NATS_ADDRESS"); v != "" {
		cfg.NATS.Address = v
	}
	if v := os.Getenv("NATS_USERNAME"); v != "" {
		cfg.NATS.Username = v
	}
	if v := os.Getenv("NATS_PASSWORD"); v != "" {
		cfg.NATS.Password = v
	}
	if v := os.Getenv("NATS_MODE"); v != "" {
		cfg.NATS.Mode = v
	}
	if v := os.Getenv("PG_DSN"); v != "" {
		cfg.Postgres.DSN = v
	}
	if v := os.Getenv("TOPIC"); v != "" {
		cfg.Topic = v
	}
	if v := os.Getenv("TABLE"); v != "" {
		cfg.Table = v
	}
	for _, setting := range []struct {
		name string
		dst  *int
	}{
		{"SAMPLE_RATE", &cfg.SampleRate},
		{"RETENTION_HOURS", &cfg.RetentionHours},
		{"MAX_RETENTION_HOURS", &cfg.MaxRetentionHours},
		{"QUERY_TIMEOUT_SECONDS", &cfg.QueryTimeoutSeconds},
		{"QUERY_MAX_ROWS", &cfg.QueryMaxRows},
	} {
		if v, set := os.LookupEnv(setting.name); set {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("%s must be an integer", setting.name)
			}
			*setting.dst = n
		}
	}
	return nil
}

// SanitizeTableName turns "example.events.order.created" into "example_events_order_created".
func SanitizeTableName(topic string) string {
	out := make([]byte, len(topic))
	for i := range len(topic) {
		c := topic[i]
		if c == '.' || c == '-' || c == '/' {
			out[i] = '_'
		} else {
			out[i] = c
		}
	}
	return string(out)
}

// SuggestedTableName builds an auto-generated PostgreSQL table name with a UTC
// creation-date suffix. Keep the suffix inside PostgreSQL's 63-byte identifier
// limit so long topic names cannot have the collision-resistant part truncated.
func SuggestedTableName(topic string, now time.Time) string {
	const maxIdentifierBytes = 63
	suffix := "_" + now.UTC().Format("20060102")
	base := SanitizeTableName(topic)
	if max := maxIdentifierBytes - len(suffix); len(base) > max {
		base = base[:max]
	}
	return base + suffix
}
