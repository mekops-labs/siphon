package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("ValidConfig", func(t *testing.T) {
		content := `
version: 2
collectors:
  test_col:
    type: "file"
    params:
      path: "/var/log/syslog"
sinks:
  test_sink:
    type: "stdout"
pipelines:
  - name: "test_pipeline"
    from: "test_col"
    parser:
      type: "jsonpath"
      vars:
        val: "$.data"
    transform:
      status: "ok"
    dispatch:
      type: "event"
      sinks:
        - name: "test_sink"
          format: "json"
          spec: "all"
`
		tmpFile := filepath.Join(tempDir, "valid.yaml")
		if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}

		cfg, err := Load(tmpFile)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cfg.Version != 2 {
			t.Errorf("Expected version 2, got %d", cfg.Version)
		}

		if col, ok := cfg.Collectors["test_col"]; !ok || col.Type != "file" {
			t.Error("Collector 'test_col' mismatch")
		}

		if len(cfg.Pipelines) != 1 {
			t.Fatal("Expected 1 pipeline")
		}

		p := cfg.Pipelines[0]

		if p.Parser.Type != "jsonpath" || p.Parser.Vars["val"] != "$.data" {
			t.Errorf("Parser configuration mismatch")
		}

		if p.Transform["status"] != "ok" {
			t.Errorf("Transform configuration mismatch")
		}
	})

	t.Run("InvalidVersion", func(t *testing.T) {
		content := `version: 1`
		tmpFile := filepath.Join(tempDir, "invalid_version.yaml")
		os.WriteFile(tmpFile, []byte(content), 0644)

		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("Expected error for version 1, got nil")
		}
	})

	t.Run("InvalidYaml", func(t *testing.T) {
		content := "invalid: yaml: ["
		tmpFile := filepath.Join(tempDir, "invalid_yaml.yaml")
		os.WriteFile(tmpFile, []byte(content), 0644)

		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("Expected error for invalid YAML, got nil")
		}
	})

	t.Run("FileNotFound", func(t *testing.T) {
		_, err := Load(filepath.Join(tempDir, "non_existent.yaml"))
		if err == nil {
			t.Fatal("Expected error for missing file, got nil")
		}
	})

	t.Run("EnvVarExpansion", func(t *testing.T) {
		os.Setenv("SIPHON_TEST_SINK", "gotify")
		defer os.Unsetenv("SIPHON_TEST_SINK")

		content := `
version: 2
sinks:
  env_sink:
    type: "%%SIPHON_TEST_SINK%%"
`
		tmpFile := filepath.Join(tempDir, "env_test.yaml")
		os.WriteFile(tmpFile, []byte(content), 0644)

		cfg, err := Load(tmpFile)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Sinks["env_sink"].Type != "gotify" {
			t.Errorf("Expected expanded type 'gotify', got '%s'", cfg.Sinks["env_sink"].Type)
		}
	})
}
