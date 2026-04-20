package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"istorage/metadata/internal/chunkers"
	"istorage/metadata/internal/config"
	"istorage/metadata/internal/gc"
	"istorage/metadata/internal/httpapi"
	"istorage/metadata/internal/service"
	fdbstore "istorage/metadata/internal/store/fdb"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))

	clusterFile, cleanup, err := ensureClusterFile(cfg.FDBClusterFile, cfg.FDBClusterFileContents)
	if err != nil {
		logger.Error("failed to prepare cluster file", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	fdb.MustAPIVersion(cfg.FDBAPIVersion)
	store, err := fdbstore.Open(clusterFile)
	if err != nil {
		logger.Error("failed to open foundationdb", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc := service.New(store)
	nodes := make([]chunkers.Node, 0, len(cfg.ChunkerNodes))
	for _, node := range cfg.ChunkerNodes {
		nodes = append(nodes, chunkers.Node{
			NodeID:  node.NodeID,
			Zone:    node.Zone,
			BaseURL: node.BaseURL,
			Healthy: true,
		})
	}
	registry := chunkers.New(nodes)
	chunkers.StartHealthChecker(ctx, logger, registry, cfg.HealthcheckInterval)
	gc.StartWorker(ctx, logger, svc, registry, cfg.GCBatchSize)

	server := &http.Server{
		Addr:    cfg.ListenAddr(),
		Handler: httpapi.NewRouter(logger, svc, registry),
	}

	logger.Info("metadata service listening", "addr", cfg.ListenAddr())
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

func ensureClusterFile(path, contents string) (string, func(), error) {
	if path != "" {
		return path, func() {}, nil
	}
	file, err := os.CreateTemp("", "fdb.cluster.*")
	if err != nil {
		return "", nil, err
	}
	if _, err := file.WriteString(contents); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return "", nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		return "", nil, err
	}
	return file.Name(), func() { _ = os.Remove(file.Name()) }, nil
}
