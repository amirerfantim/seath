package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"seath/internal/auth"
	"seath/internal/config"
	"seath/internal/httpapi"
	"seath/internal/manager"
	"seath/internal/pg"
	"seath/internal/prototypes"
	"seath/internal/recorder"
	"seath/internal/schema"
	"seath/internal/secretbox"
)

func main() {
	setupLogging()
	app := &cli.Command{
		Name:  "seath",
		Usage: "Record NATS proto messages into Postgres with configurable sampling",
		Commands: []*cli.Command{
			schemaCmd(),
			startCmd(),
			serveCmd(),
			userCmd(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

// setupLogging configures the default structured logger. Level is controlled by
// LOG_LEVEL (debug|info|warn|error), defaulting to info.
func setupLogging() {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}

func schemaCmd() *cli.Command {
	return &cli.Command{
		Name:  "schema",
		Usage: "Print proto → column mapping YAML for a topic (edit and paste into config)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "topic",
				Aliases: []string{"t"},
				Usage:   "NATS topic name (e.g. example.events.order.created)",
			},
			&cli.BoolFlag{
				Name:  "list",
				Usage: "List all known topics and exit",
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			cfg, err := config.LoadBase()
			if err != nil {
				return fmt.Errorf("config: %w", err)
			}
			db, catalogue, mappings, err := loadRuntimeCatalogue(cfg)
			if err != nil {
				return err
			}
			defer db.Close()

			if cmd.Bool("list") {
				topics := make([]string, 0, len(mappings))
				for t := range mappings {
					topics = append(topics, t)
				}
				sort.Strings(topics)
				for _, t := range topics {
					fmt.Println(t)
				}
				return nil
			}

			t := cmd.String("topic")
			if t == "" {
				return fmt.Errorf("--topic is required (or use --list to see all topics)")
			}

			protoName, ok := mappings[t]
			if !ok {
				return fmt.Errorf("unknown topic %q — run `schema --list` to see all topics", t)
			}
			msg, err := catalogue.New(protoName)
			if err != nil {
				return fmt.Errorf("topic %q: %w", t, err)
			}

			cols := schema.Introspect(msg)
			out, err := yaml.Marshal(map[string]interface{}{"columns": cols})
			if err != nil {
				return fmt.Errorf("marshal: %w", err)
			}

			fmt.Printf("# Column mapping for topic: %s\n", t)
			fmt.Printf("# Set include: false to skip a column.\n")
			fmt.Printf("# Rename: change the 'column' value to use a different PG column name.\n")
			fmt.Printf("# Paste this into your config.yaml under the 'columns:' key.\n\n")
			fmt.Print(string(out))
			return nil
		},
	}
}

func startCmd() *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start recording NATS messages to Postgres",
		Action: func(_ context.Context, _ *cli.Command) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("config: %w", err)
			}

			log.Printf("seath starting: topic=%q table=%q sample_rate=%d%%",
				cfg.Topic, cfg.Table, cfg.SampleRate)

			db, catalogue, mappings, err := loadRuntimeCatalogue(cfg)
			if err != nil {
				return err
			}
			defer db.Close()
			resolve := func(topicName string) (proto.Message, error) {
				protoName, ok := mappings[topicName]
				if !ok {
					return nil, fmt.Errorf("unknown topic %q", topicName)
				}
				return catalogue.New(protoName)
			}
			return recorder.Run(ctx, cfg, resolve)
		},
	}
}

func loadRuntimeCatalogue(cfg *config.Config) (*sql.DB, *prototypes.Registry, map[string]string, error) {
	db, err := pg.Connect(cfg.Postgres.DSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("pg connect: %w", err)
	}
	store, err := manager.NewStore(db, cfg.MaxRetentionHours)
	if err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("store: %w", err)
	}
	modules, err := store.LoadActiveProtoModules()
	if err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("load proto modules: %w", err)
	}
	catalogue, err := prototypes.Build(modules)
	if err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("load proto catalogue: %w", err)
	}
	if len(catalogue.List()) == 0 {
		db.Close()
		return nil, nil, nil, fmt.Errorf("no active proto modules; import and activate a descriptor bundle in Registry")
	}
	mappings, err := store.LoadCustomTopics()
	if err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("load topic mappings: %w", err)
	}
	return db, catalogue, mappings, nil
}

func serveCmd() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Run the web UI to manage recordings interactively",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Value:   ":8080",
				Usage:   "HTTP listen address",
				Sources: cli.EnvVars("HTTP_ADDR"),
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			cfg, err := config.LoadBase()
			if err != nil {
				return fmt.Errorf("config: %w", err)
			}

			db, err := pg.Connect(cfg.Postgres.DSN)
			if err != nil {
				return fmt.Errorf("pg connect: %w", err)
			}
			defer db.Close()

			store, err := manager.NewStore(db, cfg.MaxRetentionHours)
			if err != nil {
				return fmt.Errorf("store: %w", err)
			}

			if err := setupSecretKey(store); err != nil {
				return err
			}
			// auth.New creates recorder_users and recorder_audit. It must run
			// before the reader check, which requires every protected table.
			authn, err := auth.New(db)
			if err != nil {
				return fmt.Errorf("auth: %w", err)
			}

			mgr := manager.New(ctx, db, cfg.Postgres.DSN, store, cfg.MaxRetentionHours, cfg.QueryTimeoutSeconds, cfg.QueryMaxRows)
			reader, err := setupQueryReader(ctx, db, cfg.Postgres.QueryDSN)
			if err != nil {
				return err
			}
			if reader != nil {
				defer reader.db.Close()
				mgr.SetQueryReader(reader.db, reader.role)
			}
			if err := mgr.Reconcile(); err != nil {
				return fmt.Errorf("reconcile: %w", err)
			}

			if n, _ := auth.CountUsers(db); n == 0 {
				slog.Warn("no users configured — the UI will reject all logins until you run `seath user add <username>`")
			}

			addr := cmd.String("addr")
			srv := &http.Server{Addr: addr, Handler: authn.Middleware(httpapi.Handler(mgr))}

			go func() {
				slog.Info("UI listening", "addr", addr)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("http server error", "err", err)
					stop()
				}
			}()

			<-ctx.Done()
			slog.Info("shutting down")

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
			mgr.Shutdown()
			return nil
		},
	}
}

// setupSecretKey turns on preset secret encryption when SEATH_SECRET_KEY is
// set, and encrypts any preset secret still stored as plaintext.
func setupSecretKey(store *manager.Store) error {
	encoded := os.Getenv(secretbox.KeyEnv)
	if encoded == "" {
		if n, err := store.CountPlaintextPresetSecrets(); err == nil && n > 0 {
			slog.Warn("NATS preset secrets are stored unencrypted; set "+secretbox.KeyEnv+" to encrypt them", "presets", n)
		}
		return nil
	}
	box, err := secretbox.ParseKey(encoded)
	if err != nil {
		return err
	}
	n, err := store.UseSecretKey(box)
	if err != nil {
		return fmt.Errorf("encrypt preset secrets: %w", err)
	}
	if n > 0 {
		slog.Info("encrypted plaintext NATS preset secrets", "presets", n)
	}
	return nil
}

type queryReader struct {
	db   *sql.DB
	role string
}

// setupQueryReader connects the SQL-console login (QUERY_PG_DSN) and refuses
// to start if it is not strictly less privileged than the main login. With no
// DSN it returns nil, and only admins may use the SQL console.
func setupQueryReader(ctx context.Context, main *sql.DB, dsn string) (*queryReader, error) {
	if dsn == "" {
		slog.Warn("QUERY_PG_DSN is not set; the SQL console is limited to admins")
		return nil, nil
	}
	reader, err := pg.Connect(dsn)
	if err != nil {
		return nil, fmt.Errorf("query reader connect: %w", err)
	}
	// Viewers share this pool; cap it so they cannot use up max_connections
	// that the recorders need.
	reader.SetMaxOpenConns(4)
	check, err := pg.CheckQueryReader(ctx, main, reader)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("check query reader: %w", err)
	}
	if len(check.Problems) > 0 {
		reader.Close()
		return nil, fmt.Errorf("QUERY_PG_DSN role %q is not safe for user SQL: %s", check.Role, strings.Join(check.Problems, "; "))
	}
	slog.Info("SQL console uses a separate read-only login", "role", check.Role)
	return &queryReader{db: reader, role: check.Role}, nil
}

func userCmd() *cli.Command {
	withDB := func(fn func(db *sql.DB) error) error {
		cfg, err := config.LoadBase()
		if err != nil {
			return fmt.Errorf("config: %w", err)
		}
		db, err := pg.Connect(cfg.Postgres.DSN)
		if err != nil {
			return fmt.Errorf("pg connect: %w", err)
		}
		defer db.Close()
		if err := auth.EnsureUsersTable(db); err != nil {
			return err
		}
		return fn(db)
	}

	return &cli.Command{
		Name:  "user",
		Usage: "Manage UI login users",
		Commands: []*cli.Command{
			{
				Name:      "add",
				Usage:     "Add or update a user: user add <username> [--password ...]",
				ArgsUsage: "<username>",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "password",
						Aliases: []string{"p"},
						Usage:   "password (if omitted, read from stdin)",
					},
					&cli.StringFlag{
						Name:    "role",
						Aliases: []string{"r"},
						Value:   "admin",
						Usage:   "role: admin (full access) or viewer (read-only)",
					},
				},
				Action: func(_ context.Context, cmd *cli.Command) error {
					username := cmd.Args().First()
					if username == "" {
						return fmt.Errorf("username is required: user add <username>")
					}
					password := cmd.String("password")
					if password == "" {
						fmt.Print("password: ")
						if _, err := fmt.Scanln(&password); err != nil {
							return fmt.Errorf("read password: %w", err)
						}
					}
					role := cmd.String("role")
					return withDB(func(db *sql.DB) error {
						if err := auth.AddUser(db, username, password, role); err != nil {
							return err
						}
						fmt.Printf("user %q saved (role: %s)\n", username, role)
						return nil
					})
				},
			},
			{
				Name:  "list",
				Usage: "List users",
				Action: func(_ context.Context, _ *cli.Command) error {
					return withDB(func(db *sql.DB) error {
						users, err := auth.ListUsers(db)
						if err != nil {
							return err
						}
						if len(users) == 0 {
							fmt.Println("(no users)")
							return nil
						}
						for _, u := range users {
							fmt.Printf("%s\t%s\n", u.Name, u.Role)
						}
						return nil
					})
				},
			},
			{
				Name:      "rm",
				Usage:     "Remove a user: user rm <username>",
				ArgsUsage: "<username>",
				Action: func(_ context.Context, cmd *cli.Command) error {
					username := cmd.Args().First()
					if username == "" {
						return fmt.Errorf("username is required: user rm <username>")
					}
					return withDB(func(db *sql.DB) error {
						removed, err := auth.RemoveUser(db, username)
						if err != nil {
							return err
						}
						if !removed {
							return fmt.Errorf("user %q not found", username)
						}
						fmt.Printf("user %q removed\n", username)
						return nil
					})
				},
			},
		},
	}
}
