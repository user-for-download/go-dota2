package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/user-for-download/go-dota2/internal/bootstrap"
	"github.com/user-for-download/go-dota2/internal/config"
)

var filenameRe = regexp.MustCompile(`^(\d+)_[A-Za-z0-9_\-]+\.sql$`)

type migration struct {
	version  int
	filename string
	sql      string
}

func main() {
	log := bootstrap.NewLogger(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load("")
	must(log, "config", err)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := bootstrap.InitTelemetry(ctx, "go-dota2-migrator", cfg.Telemetry.Endpoint, cfg.Telemetry.SampleRate)
	if err != nil {
		log.Error("init telemetry", "err", err)
	} else if shutdownTelemetry != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTelemetry(shutdownCtx)
		}()
	}

	dsn := cfg.Migrator.DSN
	if dsn == "" {
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			getEnv("POSTGRES_HOST", "postgres"),
			getEnv("POSTGRES_PORT", "5432"),
			getEnv("POSTGRES_USER", "dota2"),
			getEnv("POSTGRES_PASSWORD", "dota2"),
			getEnv("POSTGRES_DB", "dota2"),
			getEnv("POSTGRES_SSLMODE", "disable"),
		)
	}
	if dsn == "" {
		log.Error("migrator: DSN required")
		os.Exit(1)
	}

	dir := cfg.Migrator.MigrationsDir
	if dir == "" {
		dir = getEnv("MIGRATIONS_DIR", "/migrations")
	}

	db, err := openWithRetry(ctx, dsn, 30*time.Second, log)
	must(log, "open db", err)
	defer db.Close()

	conn, err := db.Conn(ctx)
	must(log, "acquire dedicated connection", err)
	defer conn.Close()

	locked, err := tryAdvisoryLock(ctx, conn)
	must(log, "advisory lock", err)
	if !locked {
		log.Error("migrator: another instance is already running")
		os.Exit(1)
	}

	if err := ensureTable(ctx, conn); err != nil {
		log.Error("ensure schema_migrations", "err", err)
		os.Exit(1)
	}

	migs, err := loadMigrations(dir)
	must(log, "load migrations", err)
	log.Info("migrations discovered", "dir", dir, "count", len(migs))

	applied, err := loadApplied(ctx, conn)
	must(log, "load applied", err)

	pending := 0
	for _, m := range migs {
		if _, ok := applied[m.version]; ok {
			log.Info("skip (applied)", "version", m.version, "file", m.filename)
			continue
		}
		log.Info("applying", "version", m.version, "file", m.filename)
		if err := apply(ctx, conn, m); err != nil {
			log.Error("apply failed", "version", m.version, "file", m.filename, "err", err)
			os.Exit(1)
		}
		pending++
	}

	log.Info("done", "applied", pending, "total", len(migs))
}

func tryAdvisoryLock(ctx context.Context, conn *sql.Conn) (bool, error) {
	var locked bool
	err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(13624)").Scan(&locked)
	return locked, err
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func openWithRetry(ctx context.Context, dsn string, max time.Duration, log *slog.Logger) (*sql.DB, error) {
	deadline := time.Now().Add(max)
	var lastErr error
	for {
		db, err := sql.Open("pgx", dsn)
		if err == nil {
			if err = db.PingContext(ctx); err == nil {
				return db, nil
			}
			db.Close()
			lastErr = err
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("db not reachable after %s: %w", max, lastErr)
		}
		log.Warn("db not ready; retrying", "err", lastErr)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func ensureTable(ctx context.Context, conn *sql.Conn) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INT PRIMARY KEY,
    filename   TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`
	_, err := conn.ExecContext(ctx, ddl)
	return err
}

func loadMigrations(dir string) ([]migration, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	var out []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		match := filenameRe.FindStringSubmatch(e.Name())
		if match == nil {
			continue
		}
		v, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("bad version in %q: %w", e.Name(), err)
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", e.Name(), err)
		}
		out = append(out, migration{version: v, filename: e.Name(), sql: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })

	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d: %s and %s",
				out[i].version, out[i-1].filename, out[i].filename)
		}
	}
	return out, nil
}

func loadApplied(ctx context.Context, conn *sql.Conn) (map[int]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT version, filename FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]string{}
	for rows.Next() {
		var v int
		var f string
		if err := rows.Scan(&v, &f); err != nil {
			return nil, err
		}
		out[v] = f
	}
	return out, rows.Err()
}

func apply(ctx context.Context, conn *sql.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return fmt.Errorf("exec %s: %w", m.filename, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO schema_migrations (version, filename, applied_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (version) DO NOTHING
	`, m.version, m.filename)
	if err != nil {
		return fmt.Errorf("record %s: %w", m.filename, err)
	}
	return tx.Commit()
}

func must(log *slog.Logger, what string, err error) {
	if err != nil {
		log.Error(what, "err", err)
		os.Exit(1)
	}
}