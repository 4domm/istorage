package coordinator

import (
	"os"
	"time"

	"github.com/4domm/images/internal/common"
)

type Config struct {
	ListenAddr         string        `yaml:"listen_addr"`
	PublicURL          string        `yaml:"public_url"`
	DBPath             string        `yaml:"db_path"`
	PackSizeBytes      int64         `yaml:"pack_size_bytes"`
	ReplicaCount       int           `yaml:"replica_count"`
	HTTPTimeout        time.Duration `yaml:"http_timeout"`
	MaxUploadBytes     int64         `yaml:"max_upload_bytes"`
	ObjectCacheEntries int           `yaml:"object_cache_entries"`
	ShardID            int           `yaml:"shard_id"`
	ShardCount         int           `yaml:"shard_count"`
	ShardURLs          []string      `yaml:"shard_urls"`
}

func defaultConfig() Config {
	return Config{
		ListenAddr:         "0.0.0.0:9000",
		PublicURL:          "http://127.0.0.1:9000",
		DBPath:             "data/coordinator/badger",
		PackSizeBytes:      32 << 30,
		ReplicaCount:       3,
		HTTPTimeout:        10 * time.Second,
		MaxUploadBytes:     64 << 20,
		ObjectCacheEntries: 10000,
		ShardID:            0,
		ShardCount:         1,
		ShardURLs:          []string{"http://127.0.0.1:9000"},
	}
}

func LoadConfig() (Config, error) {
	cfg := defaultConfig()
	if err := common.LoadYAMLConfig("deploy/configs/coordinator.yaml", &cfg); err != nil && !os.IsNotExist(err) {
		return Config{}, err
	}
	return cfg, nil
}
