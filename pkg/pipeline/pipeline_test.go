package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
func (m *mockSink) Close() error        { return nil }

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
		var res map[string]any
		json.Unmarshal(data, &res)
		if res["msg"] != "test data" || res["logs"].(map[string]any)["msg"] != "test data" {
			t.Errorf("expected '{\"msg\":\"test data\", \"logs\":{\"msg\":\"test data\"}}', got '%s'", string(data))
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
		Transform: []map[string]string{
			{"sumVal": "(sumVal ?? 0) + value"}, // Accumulate sum
		},
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "template", Spec: "Current sum: {{.sumVal}}"},
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
			{
				Name:   "test",
				Format: "template",
				Spec: "Values: {{if not .cron1.value}}nil{{else}}{{.cron1.value}}{{end}}, " +
					"{{if not .cron2.value}}nil{{else}}{{.cron2.value}}{{end}}",
			},
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

	// Wait for first cron tick to process the first publish (cron1=1)
	select {
	case data := <-received:
		if string(data) != "Values: 1, nil" {
			t.Errorf("Expected 'Values: 1, nil', got '%s'", string(data))
		}
	case <-time.After(1100 * time.Millisecond):
		t.Fatal("Timed out waiting for first cron dispatch")
	}
	// This should process the second publish (cron2=2)
	select {
	case data := <-received:
		// Stateless: cron1 value is no longer in scope for this event's dispatch
		if string(data) != "Values: nil, 2" {
			t.Errorf("Expected 'Values: nil, 2', got '%s'", string(data))
		}
	case <-time.After(10 * time.Millisecond): // Short timeout for second event from same tick
		t.Fatal("Timed out waiting for second dispatch from first cron tick")
	}

	// Publish another event after the first tick
	b.Publish("cron1", []byte(`{"value": 3}`))

	// Wait for second cron tick (should process only the new event)
	select {
	case data := <-received:
		if string(data) != "Values: 3, nil" {
			t.Errorf("Expected 'Values: 3, nil', got '%s'", string(data))
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
			config.PipelineConfig{Transform: []map[string]string{{"err": "invalid syntax ("}}},
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
		Transform: []map[string]string{
			{"doubled": "val * 2"},
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
	state := r.updateStateFromEvent(cp, "logs", event) // Pass topic "logs"
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
		Transform: []map[string]string{
			{"msg_count": "(msg_count ?? 0) + 1"},
			{"avg_temp": "((avg_temp ?? 0) * (msg_count - 1) + temp) / msg_count"}, // Simple running average
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

	go r.runCronPipeline(ctx, cp)
	time.Sleep(20 * time.Millisecond) // Allow subscription

	// Send events
	b.Publish("sensor-data", []byte(`{"temp": 20}`)) // State: avg=20, count=1
	b.Publish("sensor-data", []byte(`{"temp": 25}`)) // State: avg=22.5, count=2

	time.Sleep(100 * time.Millisecond)

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
	time.Sleep(100 * time.Millisecond)

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

	r.updateStateFromEvent(cp, "some_topic", bus.Event{ // Pass a dummy topic
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
		Transform: []map[string]string{
			{"bad": "no_such_var + 1"}, // Should fail at runtime as env is empty
			{"ok": "10"},
		},
		Sinks: []config.PipelineSinkConfig{{Name: "test", Format: "template", Spec: "{{.ok}}"}},
	}
	cp, _ := r.compile(cfg)

	event := bus.Event{Payload: []byte("{}"), Ack: func() {}} // Pass a dummy topic
	state := r.updateStateFromEvent(cp, "some_topic", event)
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

	event := bus.Event{Payload: []byte("original raw payload"), Ack: func() {}} // Pass topic "mock"
	state := r.updateStateFromEvent(cp, "mock", event)
	if state != nil {
		r.dispatchState(cp, state)
	} else {
		t.Fatal("expected state to be returned from updateStateFromEvent")
	}

	// Stateless raw output includes top-level variables and nested topic data
	var res map[string]any
	json.Unmarshal(<-received, &res)
	if res["mock"].(map[string]any)["val"] != 42.0 {
		t.Errorf("Unexpected raw output: %+v", res)
	}

}

func TestRunner_StatefulEvent_CrossTopicPersistence(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 5)
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	cfg := config.PipelineConfig{
		Name:     "cross-topic",
		Topics:   []string{"t1", "t2"},
		Stateful: true,
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "expr", Spec: "{v1: t1.val, v2: t2?.val}"},
		},
	}

	parser.Register("json", func() parser.Parser { return &mockJSONParser{extractField: "val"} })
	cfg.Parser = &config.ParserConfig{Type: "json"}

	cp, _ := r.compile(cfg)
	go r.runEventPipeline(ctx, cp)
	time.Sleep(20 * time.Millisecond)

	// Publish to first topic
	b.Publish("t1", []byte(`{"val": 100}`))
	<-received // skip first result

	// Publish to second topic
	b.Publish("t2", []byte(`{"val": 200}`))
	data := <-received

	var res map[string]any
	json.Unmarshal(data, &res)

	// Both should be present because it is stateful
	if res["v1"] != 100.0 || res["v2"] != 200.0 {
		t.Errorf("Expected stateful persistence (v1=100, v2=200), got: %+v", res)
	}
}

// TestRunner_StatefulEvent_AccumulationInitialization tests that accumulating variables
// are correctly initialized to 0.0 in stateful pipelines, preventing nil+value issues.
func TestRunner_StatefulEvent_AccumulationInitialization(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 3) // Expecting 3 dispatches
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := config.PipelineConfig{
		Topics:   []string{"data-stream"},
		Name:     "stateful-event-pipeline-init",
		Stateful: true,
		Parser:   &config.ParserConfig{Type: "json"},
		Transform: []map[string]string{
			{"sum": "(sum ?? 0) + value"}, // Accumulate sum, 'sum' should be auto-initialized to 0.0
		},
		Sinks: []config.PipelineSinkConfig{
			{Name: "test", Format: "template", Spec: "Current sum: {{.sum}}"},
		},
	}

	parser.Register("json", func() parser.Parser {
		return &mockJSONParser{extractField: "value"}
	})

	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	go r.runEventPipeline(ctx, cp)
	time.Sleep(20 * time.Millisecond) // Allow subscription

	valueToSend := []int{10, 5, 20}
	expectedSums := []string{"Current sum: 10", "Current sum: 15", "Current sum: 35"}
	for i, expected := range expectedSums {
		b.Publish("data-stream", []byte(fmt.Sprintf(`{"value": %d}`, valueToSend[i]))) // Trigger next dispatch
		select {
		case data := <-received:
			if string(data) != expected {
				t.Errorf("Dispatch %d: Expected '%s', got '%s'", i+1, expected, string(data))
			}
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("Dispatch %d: Timed out waiting for dispatch", i+1)
		}
	}
}

func TestRunner_Close_CallsSinkClose(t *testing.T) {
	closed := make(chan struct{}, 2)
	mkSink := func() *mockCloseSink { return &mockCloseSink{closed: closed} }

	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{
		"alpha": mkSink(),
		"beta":  mkSink(),
	})

	r.Close()

	count := 0
	for count < 2 {
		select {
		case <-closed:
			count++
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Close() only called %d/%d sink Close() methods", count, 2)
		}
	}
}

func TestRunner_DispatchState_NilState(t *testing.T) {
	received := make(chan []byte, 1)
	ms := &mockSinkWithChannel{received: received}
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{"test": ms})

	cfg := config.PipelineConfig{
		Name:  "nil-state-test",
		Sinks: []config.PipelineSinkConfig{{Name: "test"}},
	}
	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	r.dispatchState(cp, nil) // must be a no-op

	select {
	case data := <-received:
		t.Errorf("expected no dispatch for nil state, got: %s", data)
	case <-time.After(50 * time.Millisecond):
		// expected — nothing dispatched
	}
}

func TestRunner_DispatchState_EmptySinks_PublishesToBus(t *testing.T) {
	b := bus.NewMemoryBus()
	r := NewRunner(b, make(map[string]sink.Sink))

	cfg := config.PipelineConfig{Name: "empty-sinks"}
	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	ch := b.Subscribe("empty-sinks")

	r.dispatchState(cp, map[string]any{"key": "value"})

	select {
	case event := <-ch:
		if !bytes.Contains(event.Payload, []byte("value")) {
			t.Errorf("unexpected payload: %s", event.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected bus publish for pipeline with no sinks")
	}
}

func TestRunner_DispatchToSinks_ErrorContinues(t *testing.T) {
	// First sink errors, second sink should still receive the message.
	received := make(chan []byte, 1)
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{
		"erroring": &errorSink{},
		"good":     &mockSinkWithChannel{received: received},
	})

	cfg := config.PipelineConfig{
		Sinks: []config.PipelineSinkConfig{
			{Name: "erroring"},
			{Name: "good"},
		},
	}
	cp, err := r.compile(cfg)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	r.dispatchToSinks(cp, map[string]any{"x": 1})

	select {
	case data := <-received:
		if len(data) == 0 {
			t.Error("expected non-empty payload from good sink")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("good sink did not receive dispatch after erroring sink failed")
	}
}

func TestRunner_Start_PartialFailure(t *testing.T) {
	b := bus.NewMemoryBus()
	r := NewRunner(b, map[string]sink.Sink{"stdout": &mockSink{}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelines := []config.PipelineConfig{
		{Name: "good", Topics: []string{"t"}},
		{Name: "bad", Parser: &config.ParserConfig{Type: "no-such-parser"}},
	}

	started, warnings := r.Start(ctx, pipelines)

	if started != 1 {
		t.Errorf("expected 1 started pipeline, got %d", started)
	}
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if len(warnings) > 0 && warnings[0] == "" {
		t.Error("expected non-empty warning message")
	}
}

// mockCloseSink signals on its channel when Close() is called.
type mockCloseSink struct {
	closed chan struct{}
}

func (m *mockCloseSink) Send(b []byte) error { return nil }
func (m *mockCloseSink) Close() error {
	m.closed <- struct{}{}
	return nil
}

// errorSink always returns an error from Send.
type errorSink struct{}

func (e *errorSink) Send(b []byte) error { return errors.New("send failed") }
func (e *errorSink) Close() error        { return nil }

// mockSinkWithChannel implements the sink.Sink interface for testing with a channel
type mockSinkWithChannel struct {
	received chan []byte
}

func (m *mockSinkWithChannel) Send(b []byte) error {
	m.received <- b
	return nil
}

func (m *mockSinkWithChannel) Close() error { return nil }

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
