package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	APIServiceAddr     string
	MetadataServiceURL string
	ChunkSize          int
	GetBatchWindow     int
	LogLevel           string
}

func Load() (Config, error) {
	cfg := defaultConfig()
	path := envString("CONFIG_PATH", "config.yaml")
	if raw, err := os.ReadFile(path); err == nil {
		applySimpleYAML(&cfg, string(raw))
	} else if !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("read config %s: %w", filepath.Clean(path), err)
	}
	applyEnvOverrides(&cfg)
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		APIServiceAddr:     "0.0.0.0:3000",
		MetadataServiceURL: "http://127.0.0.1:3001",
		ChunkSize:          1024 * 1024,
		GetBatchWindow:     16,
		LogLevel:           envString("LOG_LEVEL", "info"),
	}
}

func applySimpleYAML(cfg *Config, raw string) {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), "\"'")
		switch key {
		case "api_service_addr":
			cfg.APIServiceAddr = value
		case "metadata_service_url":
			cfg.MetadataServiceURL = strings.TrimRight(value, "/")
		case "chunk_size":
			if v, err := strconv.Atoi(value); err == nil {
				cfg.ChunkSize = v
			}
		case "get_batch_window":
			if v, err := strconv.Atoi(value); err == nil {
				cfg.GetBatchWindow = v
			}
		}
	}
}

func applyEnvOverrides(cfg *Config) {
	if value := os.Getenv("API_GET_BATCH_WINDOW"); value != "" {
		if v, err := strconv.Atoi(value); err == nil {
			cfg.GetBatchWindow = v
		}
	}
	if value := os.Getenv("API_SERVICE_ADDR"); value != "" {
		cfg.APIServiceAddr = value
	}
	if value := os.Getenv("METADATA_SERVICE_URL"); value != "" {
		cfg.MetadataServiceURL = strings.TrimRight(value, "/")
	}
	if value := os.Getenv("API_CHUNK_SIZE"); value != "" {
		if v, err := strconv.Atoi(value); err == nil {
			cfg.ChunkSize = v
		}
	}
	if value := os.Getenv("API_SERVICE_LOG_FILTER"); value != "" {
		cfg.LogLevel = value
	}
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
