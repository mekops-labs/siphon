package main

import (
	"errors"
	"testing"

	"github.com/mekops-labs/siphon/internal/config"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mekops-labs/siphon/pkg/sink"
)

// testCollector is a no-op collector for testing.
type testCollector struct{}

func (c *testCollector) Start(b bus.Bus)                   {}
func (c *testCollector) End()                              {}
func (c *testCollector) RegisterTopic(name, value string)  {}

// testSink is a no-op sink for testing.
type testSink struct{}

func (s *testSink) Send(b []byte) error { return nil }
func (s *testSink) Close() error        { return nil }

func init() {
	collector.Registry.Add("test-collector", func(params any) collector.Collector {
		return &testCollector{}
	})
	sink.Registry.Add("test-sink", func(params any, b bus.Bus) (sink.Sink, error) {
		return &testSink{}, nil
	})
	sink.Registry.Add("test-sink-error", func(params any, b bus.Bus) (sink.Sink, error) {
		return nil, errors.New("init failed")
	})
}

func TestBuildCollectors_KnownType(t *testing.T) {
	cfg := &config.Config{
		Collectors: map[string]config.CollectorConfig{
			"my-collector": {Type: "test-collector", Topics: map[string]string{"alias": "value"}},
		},
	}

	collectors, warnings := buildCollectors(cfg)

	if len(collectors) != 1 {
		t.Errorf("expected 1 collector, got %d", len(collectors))
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
	if _, ok := collectors["my-collector"]; !ok {
		t.Error("expected collector 'my-collector' to be present")
	}
}

func TestBuildCollectors_UnknownType(t *testing.T) {
	cfg := &config.Config{
		Collectors: map[string]config.CollectorConfig{
			"bad": {Type: "no-such-type"},
		},
	}

	collectors, warnings := buildCollectors(cfg)

	if len(collectors) != 0 {
		t.Errorf("expected 0 collectors, got %d", len(collectors))
	}
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
}

func TestBuildSinks_KnownType(t *testing.T) {
	cfg := &config.Config{
		Sinks: map[string]config.SinkConfig{
			"my-sink": {Type: "test-sink"},
		},
	}

	sinks, warnings := buildSinks(cfg, bus.NewMemoryBus())

	if len(sinks) != 1 {
		t.Errorf("expected 1 sink, got %d", len(sinks))
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
}

func TestBuildSinks_UnknownType(t *testing.T) {
	cfg := &config.Config{
		Sinks: map[string]config.SinkConfig{
			"bad": {Type: "no-such-type"},
		},
	}

	sinks, warnings := buildSinks(cfg, bus.NewMemoryBus())

	if len(sinks) != 0 {
		t.Errorf("expected 0 sinks, got %d", len(sinks))
	}
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
}

func TestBuildSinks_InitError(t *testing.T) {
	cfg := &config.Config{
		Sinks: map[string]config.SinkConfig{
			"erroring": {Type: "test-sink-error"},
		},
	}

	sinks, warnings := buildSinks(cfg, bus.NewMemoryBus())

	if len(sinks) != 0 {
		t.Errorf("expected 0 sinks, got %d", len(sinks))
	}
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning for init error, got %d: %v", len(warnings), warnings)
	}
}
