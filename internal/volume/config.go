package volume

import (
	"os"
	"time"

	"github.com/4domm/images/internal/common"
)

type Config struct {
	ServerID          string        `yaml:"server_id"`
	ListenAddr        string        `yaml:"listen_addr"`
	PublicURL         string        `yaml:"public_url"`
	CoordinatorURL    string        `yaml:"coordinator_url"`
	DataDir           string        `yaml:"data_dir"`
	MaxPackBytes      int64         `yaml:"max_pack_bytes"`
	MaxUploadBytes    int64         `yaml:"max_upload_bytes"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	HTTPTimeout       time.Duration `yaml:"http_timeout"`
}

func defaultConfig() Config {
	return Config{
		ServerID:          "storage-a",
		ListenAddr:        "0.0.0.0:9002",
		PublicURL:         "http://127.0.0.1:9002",
		CoordinatorURL:    "http://127.0.0.1:9000",
		DataDir:           "data/storage",
		MaxPackBytes:      32 << 30,
		MaxUploadBytes:    64 << 20,
		SnapshotInterval:  5 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		HTTPTimeout:       5 * time.Second,
	}
}

func LoadConfig() (Config, error) {
	cfg := defaultConfig()
	if err := common.LoadYAMLConfig("deploy/configs/volume.yaml", &cfg); err != nil && !os.IsNotExist(err) {
		return Config{}, err
	}
	return cfg, nil
}
