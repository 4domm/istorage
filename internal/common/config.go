package common

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func LoadYAMLConfig(path string, dst any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("parse yaml %s: %w", filepath.Clean(path), err)
	}
	return nil
}
