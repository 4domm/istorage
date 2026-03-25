package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port         int
	DBPath       string
	MaxBodyBytes int64
	LogLevel     string
}

func Load() Config {
	chunkSize := int64(1024 * 1024)
	path := envString("CONFIG_PATH", "config.yaml")
	if raw, err := os.ReadFile(path); err == nil {
		chunkSize = parseChunkSize(string(raw), chunkSize)
	}

	maxBodyBytes := chunkSize
	if value := os.Getenv("CHUNK_MAX_BODY_BYTES"); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			maxBodyBytes = parsed
		}
	}

	return Config{
		Port:         envInt("CHUNK_PORT", 3002),
		DBPath:       envString("CHUNK_DB_PATH", "./data/chunks"),
		MaxBodyBytes: maxBodyBytes,
		LogLevel:     envString("CHUNKER_LOG_FILTER", "info"),
	}
}

func parseChunkSize(raw string, fallback int64) int64 {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.TrimSpace(key) != "chunk_size" {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), "\"'")
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}
