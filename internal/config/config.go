package config

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
	"github.com/mekops-labs/siphon/internal/utils"
)

// Config is the root of the Siphon v2 configuration
type Config struct {
	Version    int                        `yaml:"version"`
	Collectors map[string]CollectorConfig `yaml:"collectors"`
	Sinks      map[string]SinkConfig      `yaml:"sinks"`
	Pipelines  []PipelineConfig           `yaml:"pipelines"`
}

type CollectorConfig struct {
	Type   string                 `yaml:"type"`
	Topics map[string]string      `yaml:"topics"`
	Params map[string]interface{} `yaml:"params"` // Generic params for module
}

type SinkConfig struct {
	Type   string                 `yaml:"type"`
	Params map[string]interface{} `yaml:"params"` // Generic params for module
}

// PipelineConfig defines the linear data flow
type PipelineConfig struct {
	Name     string `yaml:"name"`
	BusMode  string `yaml:"bus_mode,omitempty"` // "volatile" or "durable"
	Stateful bool   `yaml:"stateful,omitempty"`

	Type     string   `yaml:"type,omitempty"` // "cron" or empty
	Schedule string   `yaml:"schedule,omitempty"`
	Topics   []string `yaml:"topics,omitempty"`

	Parser    *ParserConfig       `yaml:"parser,omitempty"`
	Transform []map[string]string `yaml:"transform,omitempty"`

	Sinks []PipelineSinkConfig `yaml:"sinks"`
}

type ParserConfig struct {
	Type string            `yaml:"type"`
	Vars map[string]string `yaml:"vars"`
}

type PipelineSinkConfig struct {
	Name   string `yaml:"name"`
	Format string `yaml:"format"`
	Spec   string `yaml:"spec"`
}

// Load reads the YAML file, expands ENV vars, and unmarshals it into the Config struct.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("can't read config file: %w", err)
	}

	processedYaml := utils.ReplaceWithEnvVars(string(raw))

	var cfg Config
	if err := yaml.Unmarshal([]byte(processedYaml), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	if cfg.Version != 2 {
		return nil, fmt.Errorf("unsupported config version: %d (expected 2)", cfg.Version)
	}

	return &cfg, nil
}
