package chunkers

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func StartHealthChecker(ctx context.Context, logger *slog.Logger, registry *Registry, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		runHealthCheck(ctx, logger, registry, client)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runHealthCheck(ctx, logger, registry, client)
			}
		}
	}()
}

func runHealthCheck(ctx context.Context, logger *slog.Logger, registry *Registry, client *http.Client) {
	for _, node := range registry.List(false) {
		url := strings.TrimRight(node.BaseURL, "/") + "/health"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		healthy := err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300
		if resp != nil {
			_ = resp.Body.Close()
		}
		registry.SetHealth(node.NodeID, healthy)
		if !healthy {
			logger.Warn("chunker unhealthy", "node_id", node.NodeID, "base_url", node.BaseURL)
		}
	}
}
