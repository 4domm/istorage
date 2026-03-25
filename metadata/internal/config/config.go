package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultClusterFileContents = "docker:docker@foundationdb:4500"

type Config struct {
	Port                   int
	LogLevel               string
	GCBatchSize            int
	ChunkerNodes           []ChunkerNode
	HealthcheckInterval    time.Duration
	FDBClusterFile         string
	FDBClusterFileContents string
	FDBAPIVersion          int
}

type ChunkerNode struct {
	NodeID  string
	BaseURL string
}

func Load() (Config, error) {
	cfg := Config{
		Port:                   envInt("METADATA_PORT", 3001),
		LogLevel:               envString("LOG_LEVEL", "info"),
		GCBatchSize:            envInt("METADATA_GC_BATCH_SIZE", 128),
		HealthcheckInterval:    time.Duration(envInt("CHUNKER_HEALTHCHECK_INTERVAL_MS", 2000)) * time.Millisecond,
		FDBClusterFile:         os.Getenv("FDB_CLUSTER_FILE"),
		FDBClusterFileContents: envString("FDB_CLUSTER_FILE_CONTENTS", defaultClusterFileContents),
		FDBAPIVersion:          envInt("FDB_API_VERSION", 730),
	}
	nodes, err := parseChunkerNodes(envString("CHUNKER_NODES", "chunker-a=http://127.0.0.1:3002"))
	if err != nil {
		return Config{}, err
	}
	cfg.ChunkerNodes = nodes
	return cfg, nil
}

func (c Config) ListenAddr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseChunkerNodes(raw string) ([]ChunkerNode, error) {
	out := make([]ChunkerNode, 0)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		nodeID, baseURL, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid CHUNKER_NODES entry: %s", entry)
		}
		nodeID = strings.TrimSpace(nodeID)
		baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
		if nodeID == "" || baseURL == "" {
			return nil, fmt.Errorf("invalid CHUNKER_NODES entry: %s", entry)
		}
		out = append(out, ChunkerNode{NodeID: nodeID, BaseURL: baseURL})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("CHUNKER_NODES must contain at least one node")
	}
	return out, nil
}
