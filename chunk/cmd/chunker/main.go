package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"sstorage/chunk/internal/config"
	"sstorage/chunk/internal/httpapi"
	"sstorage/chunk/internal/store"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))

	db, err := store.Open(cfg.DBPath)
	if err != nil {
		logger.Error("failed to open pebble", "path", cfg.DBPath, "error", err)
		os.Exit(1)
	}
	defer db.Close()

	server := &http.Server{
		Addr:    "0.0.0.0:" + strconv.Itoa(cfg.Port),
		Handler: httpapi.New(db, cfg.MaxBodyBytes).Handler(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("chunk storage listening", "addr", server.Addr)
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func parseLogLevel(value string) slog.Level {
	switch value {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
