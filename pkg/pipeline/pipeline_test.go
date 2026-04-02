package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mekops-labs/siphon/internal/config"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/parser"
	"github.com/mekops-labs/siphon/pkg/sink"
)

// mockSink implements the sink.Sink interface for testing
type mockSink struct{}

func (m *mockSink) Send(b []byte) error { return nil }

// mockParser implements parser.Parser for testing
type mockParser struct{}

func (m *mockParser) Parse(payload []byte, vars map[string]string) (map[string]any, error) {
	return map[string]any{"val": 42}, nil
}

type errorParser struct{}

func (e *errorParser) Parse(payload []byte, vars map[string]string) (map[string]any, error) {
	return nil, errors.New("parse error")
}

func init() {
	parser.Register("mock", func() parser.Parser { return &mockParser{} })
	parser.Register("error", func() parser.Parser { return &errorParser{} })
}

func TestNewRunner(t *testing.T) {
	b := bus.NewMemoryBus()
	sinks := map[string]sink.Sink{"test": &mockSink{}}
	r := NewRunner(b, sinks)

	if r.bus != b {
		t.Errorf("expected bus to be set correctly")
	}
	if len(r.sinks) != 1 {
		t.Errorf("expected 1 sink, got %d", len(r.sinks))
	}
}

func TestRunner_Start(t *testing.T) {
	b := bus.NewMemoryBus()
	r := NewRunner(b, make(map[string]sink.Sink))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelines := []config.PipelineConfig{
		{
			Name:     "cron-test",
			Type:     "cron",
			Schedule: "0/1 * * * * *", // Every second
			Topics:   []string{"timed"},
		},
		{
			Name:   "event-test",
			Type:   "event",
			Topics: []string{"events"},
		},
	}

	// This should not block
	r.Start(ctx, pipelines)

	// Short sleep to allow goroutine for event pipeline to start
	time.Sleep(20 * time.Millisecond)

	// Verify event pipeline subscribed by publishing and seeing if it doesn't panic
	busEvents := b.Subscribe("events")
	err := b.Publish("events", []byte("data"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case event := <-busEvents:
		// Success, we received the event
		if event.Payload == nil || string(event.Payload) != "data" {
			t.Errorf("expected payload 'data', got '%s'", string(event.Payload))
		}
	case <-time.After(20 * time.Millisecond):
		t.Error("expected to receive an event but timed out")
	}

	// Verify cron pipeline is running by waiting a bit and then checking if it published to the bus
	events := b.Subscribe("timed")
	if err := b.Publish("timed", []byte("tick")); err != nil {
		t.Fatalf("failed to publish to timed topic: %v", err)
	}

	time.Sleep(1100 * time.Millisecond) // Wait a bit longer than the cron schedule

	select {
	case event := <-events:
		// Success, we received a cron event
		if event.Payload == nil || string(event.Payload) != "tick" {
			t.Errorf("expected payload 'tick', got '%s'", string(event.Payload))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected to receive a cron event but timed out")
	}
}

func TestRunner_ProcessEvent(t *testing.T) {
	var ackCalled int32
	b := bus.NewMemoryBus()
	ms := &mockSink{}
	r := NewRunner(b, map[string]sink.Sink{"stdout": ms})

	cfg := config.PipelineConfig{
		Name: "test-pipeline",
		Sinks: []config.PipelineSinkConfig{
			{Name: "stdout"},
		},
	}

	event := bus.Event{
		Topic:   "test",
		Payload: []byte("hello"),
		Ack: func() {
			atomic.AddInt32(&ackCalled, 1)
		},
	}

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	r.processEvent(cp, event)

	if atomic.LoadInt32(&ackCalled) != 1 {
		t.Error("expected event.Ack() to be called")
	}
}

func TestRunner_Compile_MissingSink(t *testing.T) {
	// Ensure compilation fails when a sink is missing
	b := bus.NewMemoryBus()
	r := NewRunner(b, make(map[string]sink.Sink))
	cfg := config.PipelineConfig{
		Name:  "test-pipeline",
		Sinks: []config.PipelineSinkConfig{{Name: "non-existent"}},
	}
	if _, err := r.compile(cfg); err == nil {
		t.Error("expected error during compilation for missing sink")
	}
}

func TestRunner_RunEventPipeline(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 1)
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	cfg := config.PipelineConfig{
		Topics: []string{"logs"},
		Name:   "logger",
		Sinks: []config.PipelineSinkConfig{
			{Name: "test"},
		},
	}

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	// Start pipeline in background
	go r.runEventPipeline(ctx, cp)

	// We need to wait a tiny bit for the subscription to happen inside the goroutine
	time.Sleep(20 * time.Millisecond)

	// Publish an event to the bus
	err = b.Publish("logs", []byte("test data"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case data := <-received:
		if string(data) != "test data" {
			t.Errorf("expected 'test data', got '%s'", string(data))
		}
	case <-time.After(20 * time.Millisecond):
		t.Error("event was not processed within timeout")
	}
}

func TestRunner_Compile_Errors(t *testing.T) {
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{"stdout": &mockSink{}})

	tests := []struct {
		name string
		cfg  config.PipelineConfig
	}{
		{
			"UnknownParser",
			config.PipelineConfig{Parser: &config.ParserConfig{Type: "unknown"}},
		},
		{
			"InvalidTransform",
			config.PipelineConfig{Transform: map[string]string{"err": "invalid syntax ("}},
		},
		{
			"InvalidSinkExpr",
			config.PipelineConfig{
				Sinks: []config.PipelineSinkConfig{{Name: "stdout", Format: "expr", Spec: "{"}},
			},
		},
		{
			"InvalidSinkTemplate",
			config.PipelineConfig{
				Sinks: []config.PipelineSinkConfig{{Name: "stdout", Format: "template", Spec: "{{bad}"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := r.compile(tt.cfg); err == nil {
				t.Errorf("expected error for %s", tt.name)
			}
		})
	}
}

func TestRunner_ProcessEvent_FullETL(t *testing.T) {
	received := make(chan []byte, 2)
	ms := &mockSinkWithChannel{received: received}
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{"test": ms})

	cfg := config.PipelineConfig{
		Parser: &config.ParserConfig{Type: "mock"}, // returns {"val": 42}
		Transform: map[string]string{
			"doubled": "val * 2",
		},
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "template", Spec: "Result: {{.doubled}}"},
			{Name: "test", Format: "expr", Spec: "{out: doubled}"},
		},
	}

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	r.processEvent(cp, bus.Event{Payload: []byte("{}"), Ack: func() {}})

	// 1. Template Output
	select {
	case data := <-received:
		if string(data) != "Result: 84" {
			t.Errorf("expected 'Result: 84', got '%s'", string(data))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for template output")
	}

	// 2. Expr Output
	select {
	case data := <-received:
		var res map[string]int
		if err := json.Unmarshal(data, &res); err != nil {
			t.Fatalf("failed to unmarshal expr output: %v", err)
		}
		if res["out"] != 84 {
			t.Errorf("expected out: 84, got %v", res["out"])
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for expr output")
	}
}

func TestRunner_ProcessEvent_ParserError(t *testing.T) {
	var nackCalled int32
	b := bus.NewMemoryBus()
	r := NewRunner(b, nil)

	cfg := config.PipelineConfig{
		Parser: &config.ParserConfig{Type: "error"},
	}
	cp, _ := r.compile(cfg)

	r.processEvent(cp, bus.Event{
		Nack: func() { atomic.StoreInt32(&nackCalled, 1) },
	})

	if atomic.LoadInt32(&nackCalled) != 1 {
		t.Error("expected Nack() to be called on parser error")
	}
}

func TestRunner_ProcessEvent_TransformError(t *testing.T) {
	received := make(chan []byte, 1)
	ms := &mockSinkWithChannel{received: received}
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{"test": ms})

	cfg := config.PipelineConfig{
		Transform: map[string]string{
			"bad": "no_such_var + 1", // Should fail at runtime as env is empty
			"ok":  "10",
		},
		Sinks: []config.PipelineSinkConfig{{Name: "test", Format: "template", Spec: "{{.ok}}"}},
	}
	cp, _ := r.compile(cfg)

	r.processEvent(cp, bus.Event{Payload: []byte("{}"), Ack: func() {}})

	if string(<-received) != "10" {
		t.Error("expected processing to continue after transform error")
	}
}

func TestRunner_ProcessEvent_RawFallback(t *testing.T) {
	received := make(chan []byte, 1)
	ms := &mockSinkWithChannel{received: received}
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{"test": ms})

	cfg := config.PipelineConfig{
		Sinks: []config.PipelineSinkConfig{{Name: "test", Format: "raw"}},
	}
	cp, _ := r.compile(cfg)

	r.processEvent(cp, bus.Event{Payload: []byte("raw"), Ack: func() {}})

	if string(<-received) != "raw" {
		t.Error("expected raw payload fallback")
	}
}

// mockSinkWithChannel implements the sink.Sink interface for testing with a channel
type mockSinkWithChannel struct {
	received chan []byte
}

func (m *mockSinkWithChannel) Send(b []byte) error {
	m.received <- b
	return nil
}
