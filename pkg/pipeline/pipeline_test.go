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

func TestRunner_Start_EventAndCronPipelines(t *testing.T) {
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

func TestRunner_RunEventPipeline_Stateless(t *testing.T) {
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
	err = b.Publish("logs", []byte(`{"msg":"test data"}`))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case data := <-received:
		if string(data) != `{"msg":"test data"}` {
			t.Errorf("expected '{\"msg\":\"test data\"}', got '%s'", string(data))
		}
	case <-time.After(20 * time.Millisecond):
		t.Error("event was not processed within timeout")
	}
}

func TestRunner_RunEventPipeline_Stateful(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 3) // Expecting 3 dispatches
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := config.PipelineConfig{
		Topics:   []string{"data-stream"},
		Name:     "stateful-event-pipeline",
		Stateful: true,
		Parser:   &config.ParserConfig{Type: "json"}, // Assuming a JSON parser that extracts "value"
		Transform: map[string]string{
			"prev": "sum",          // Keep the last value for accumulation
			"sum":  "prev + value", // Accumulate sum
		},
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "template", Spec: "Current sum: {{.sum}}"},
		},
	}

	// Register a mock JSON parser that extracts a "value" field
	parser.Register("json", func() parser.Parser {
		return &mockJSONParser{
			extractField: "value",
		}
	})

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	// Initialize sum to 0
	cp.state["prev"] = 0.0
	cp.state["sum"] = 0.0
	cp.state["sum"] = 0.0  // Initialize sum for the first iteration
	cp.state["prev"] = 0.0 // Initialize prev as well, though it will be overwritten by "sum"

	// Start pipeline in background

	go r.runEventPipeline(ctx, cp)
	time.Sleep(20 * time.Millisecond) // Allow subscription

	// Send first event
	b.Publish("data-stream", []byte(`{"value": 10}`))
	select {
	case data := <-received:
		if string(data) != "Current sum: 10" {
			t.Errorf("Expected 'Current sum: 10', got '%s'", string(data))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Timed out waiting for first dispatch")
	}

	// Send second event
	b.Publish("data-stream", []byte(`{"value": 5}`))
	select {
	case data := <-received:
		if string(data) != "Current sum: 15" {
			t.Errorf("Expected 'Current sum: 15', got '%s'", string(data))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Timed out waiting for second dispatch")
	}

	// Send third event
	b.Publish("data-stream", []byte(`{"value": 20}`))
	select {
	case data := <-received:
		if string(data) != "Current sum: 35" {
			t.Errorf("Expected 'Current sum: 35', got '%s'", string(data))
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Timed out waiting for third dispatch")
	}
}

func TestRunner_RunCronPipeline_Stateless(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 2) // Expecting 2 dispatches
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond) // Longer timeout for cron
	defer cancel()

	cfg := config.PipelineConfig{
		Topics:   []string{"cron1", "cron2"},
		Name:     "stateless-cron-pipeline",
		Type:     "cron",
		Schedule: "0/1 * * * * *", // Every second
		Stateful: false,           // Explicitly stateless
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "template", Spec: "Value: {{.value}}"},
		},
	}

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	go r.runCronPipeline(ctx, cp)
	time.Sleep(20 * time.Millisecond) // Allow subscription

	// Publish events
	b.Publish("cron1", []byte(`{"value": 1}`))
	b.Publish("cron2", []byte(`{"value": 2}`))

	// Wait for first cron tick (should process both events)
	select {
	case data := <-received:
		if string(data) != "Value: 1" && string(data) != "Value: 2" {
			t.Errorf("Expected 'Value: 1' or 'Value: 2', got '%s'", string(data))
		}
	case <-time.After(1100 * time.Millisecond):
		t.Fatal("Timed out waiting for first cron dispatch")
	}
	select {
	case data := <-received:
		if string(data) != "Value: 1" && string(data) != "Value: 2" {
			t.Errorf("Expected 'Value: 1' or 'Value: 2', got '%s'", string(data))
		}
	case <-time.After(10 * time.Millisecond): // Short timeout for second event from same tick
		t.Fatal("Timed out waiting for second dispatch from first cron tick")
	}

	// Publish another event after the first tick
	b.Publish("cron1", []byte(`{"value": 3}`))

	// Wait for second cron tick (should process only the new event)
	select {
	case data := <-received:
		if string(data) != "Value: 3" {
			t.Errorf("Expected 'Value: 3', got '%s'", string(data))
		}
	case <-time.After(1100 * time.Millisecond):
		t.Fatal("Timed out waiting for second cron dispatch")
	}

	// Ensure no more events are processed from previous ticks
	select {
	case data := <-received:
		t.Errorf("Unexpected dispatch: %s", string(data))
	case <-time.After(50 * time.Millisecond):
		// Expected, no more events should be processed
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

func TestRunner_FullETL_Stateless(t *testing.T) {
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

	event := bus.Event{Payload: []byte("{}"), Ack: func() {}}
	state := r.updateStateFromEvent(cp, event)
	if state != nil {
		r.dispatchState(cp, state)
	} else {
		t.Fatal("expected state to be returned from updateStateFromEvent")
	}

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

func TestRunner_RunCronPipeline_Stateful(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 3) // Expecting 3 dispatches
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 3500*time.Millisecond) // Longer timeout for cron
	defer cancel()

	cfg := config.PipelineConfig{
		Topics:   []string{"sensor-data"},
		Name:     "stateful-cron-pipeline",
		Type:     "cron",
		Schedule: "0/1 * * * * *", // Every second
		Stateful: true,
		Parser:   &config.ParserConfig{Type: "json"}, // Assuming a JSON parser that extracts "temp"
		Transform: map[string]string{
			"old_avg":   "avg_temp",
			"hits":      "msg_count",
			"msg_count": "msg_count + 1",
			"avg_temp":  "(old_avg * hits + temp) / (hits + 1)", // Simple running average
		},
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "template", Spec: "Avg Temp: {{.avg_temp | printf \"%.2f\"}}, Count: {{.msg_count}}"},
		},
	}

	// Register a mock JSON parser that extracts a "temp" field
	parser.Register("json", func() parser.Parser {
		return &mockJSONParser{
			extractField: "temp",
		}
	})

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	// Initialize state for running average
	cp.state["avg_temp"] = 0.0
	cp.state["msg_count"] = 0.0

	go r.runCronPipeline(ctx, cp)
	time.Sleep(20 * time.Millisecond) // Allow subscription

	// Send events
	b.Publish("sensor-data", []byte(`{"temp": 20}`)) // State: avg=20, count=1
	time.Sleep(50 * time.Millisecond)
	b.Publish("sensor-data", []byte(`{"temp": 25}`)) // State: avg=22.5, count=2
	time.Sleep(50 * time.Millisecond)

	// First cron tick (after ~1s)
	select {
	case data := <-received:
		if string(data) != "Avg Temp: 22.50, Count: 2" {
			t.Errorf("Expected 'Avg Temp: 22.50, Count: 2', got '%s'", string(data))
		}
	case <-time.After(1100 * time.Millisecond):
		t.Fatal("Timed out waiting for first cron dispatch")
	}

	b.Publish("sensor-data", []byte(`{"temp": 30}`)) // State: avg=25, count=3
	time.Sleep(50 * time.Millisecond)

	// Second cron tick (after ~2s)
	select {
	case data := <-received:
		if string(data) != "Avg Temp: 25.00, Count: 3" {
			t.Errorf("Expected 'Avg Temp: 25.00, Count: 3', got '%s'", string(data))
		}
	case <-time.After(1100 * time.Millisecond):
		t.Fatal("Timed out waiting for second cron dispatch")
	}
}

func TestRunner_ParserError(t *testing.T) {
	var nackCalled int32
	b := bus.NewMemoryBus()
	r := NewRunner(b, nil)

	cfg := config.PipelineConfig{
		Parser: &config.ParserConfig{Type: "error"},
		Sinks:  []config.PipelineSinkConfig{}, // Ensure no dispatch happens
	}
	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	r.updateStateFromEvent(cp, bus.Event{
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

	event := bus.Event{Payload: []byte("{}"), Ack: func() {}}
	state := r.updateStateFromEvent(cp, event)
	if state != nil {
		r.dispatchState(cp, state)
	} else {
		t.Fatal("expected state to be returned from updateStateFromEvent")
	}

	if string(<-received) != "10" { // Expecting "10" from the 'ok' transform
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
	// Use mock parser to ensure state is populated
	cfg.Parser = &config.ParserConfig{Type: "mock"} // This parser returns {"val": 42}

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	event := bus.Event{Payload: []byte("original raw payload"), Ack: func() {}}
	state := r.updateStateFromEvent(cp, event)
	if state != nil {
		r.dispatchState(cp, state)
	} else {
		t.Fatal("expected state to be returned from updateStateFromEvent")
	}

	expectedOutput := `{"val":42}` // The mock parser sets "val": 42, and "raw" format now marshals the state.

	select {
	case data := <-received:
		if string(data) != expectedOutput {
			t.Errorf("expected '%s', got '%s'", expectedOutput, string(data))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for raw fallback output")
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

// mockJSONParser implements parser.Parser for testing JSON extraction
type mockJSONParser struct {
	extractField string
}

func (m *mockJSONParser) Parse(payload []byte, vars map[string]string) (map[string]any, error) {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	if val, ok := data[m.extractField]; ok {
		return map[string]any{m.extractField: val}, nil
	}
	return map[string]any{}, nil
}
