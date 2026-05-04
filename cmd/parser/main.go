package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user-for-download/go-dota2/internal/bootstrap"
	"github.com/user-for-download/go-dota2/internal/config"
)

func main() {
	log := bootstrap.NewLogger(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load("")
	must(log, "config", err)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := bootstrap.InitTelemetry(ctx, "go-dota2-parser", cfg.Telemetry.Endpoint, cfg.Telemetry.SampleRate)
	if err != nil {
		log.Error("init telemetry", "err", err)
	} else if shutdownTelemetry != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTelemetry(shutdownCtx)
		}()
	}

	core, err := bootstrap.Core(cfg, log)
	must(log, "core", err)
	defer core.Close()

	db, err := bootstrap.WaitForPostgres(ctx, cfg.Postgres, bootstrap.WaitConfig{
		Timeout:      0,
		PollInterval: 30 * time.Second,
	}, log)
	must(log, "postgres", err)
	defer db.Close()

	parserDeps, err := bootstrap.BuildParser(ctx, cfg, log, core, db)
	must(log, "build parser", err)

	log.Info("parser: starting")
	if err := parserDeps.Worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("parser: stopped", "err", err)
	}
	log.Info("parser: stopped cleanly")
}

func must(log *slog.Logger, what string, err error) {
	if err != nil {
		log.Error(what, "err", err)
		os.Exit(1)
	}
}