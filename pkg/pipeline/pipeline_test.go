package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mekops-labs/siphon/internal/config"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
)

// mockSink implements the sink.Sink interface for testing
type mockSink struct{}

func (m *mockSink) Send(b []byte) error { return nil }

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
			Name: "cron-test",
			Type: "cron",
		},
		{
			Name:        "event-test",
			Type:        "event",
			SourceTopic: "telemetry",
		},
	}

	// This should not block
	r.Start(ctx, pipelines)

	// Short sleep to allow goroutine for event pipeline to start
	time.Sleep(20 * time.Millisecond)

	// Verify event pipeline subscribed by publishing and seeing if it doesn't panic
	err := b.Publish("telemetry", []byte("data"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
}

func TestRunner_ProcessEvent(t *testing.T) {
	var ackCalled int32
	b := bus.NewMemoryBus()
	ms := &mockSink{}
	r := NewRunner(b, map[string]sink.Sink{"stdout": ms})

	cfg := config.PipelineConfig{
		Name: "test-pipeline",
		Dispatch: &config.DispatchConfig{
			Sinks: []config.PipelineSinkConfig{
				{Name: "stdout"},
			},
		},
	}

	event := bus.Event{
		Topic:   "test",
		Payload: []byte("hello"),
		Ack: func() {
			atomic.AddInt32(&ackCalled, 1)
		},
	}

	r.processEvent(cfg, event)

	if atomic.LoadInt32(&ackCalled) != 1 {
		t.Error("expected event.Ack() to be called")
	}
}

func TestRunner_ProcessEvent_MissingSink(t *testing.T) {
	// Ensure it doesn't panic when a sink is missing
	b := bus.NewMemoryBus()
	r := NewRunner(b, make(map[string]sink.Sink))

	cfg := config.PipelineConfig{
		Name: "test-pipeline",
		Dispatch: &config.DispatchConfig{
			Sinks: []config.PipelineSinkConfig{
				{Name: "non-existent"},
			},
		},
	}

	event := bus.Event{
		Ack: func() {},
	}

	// Should log an error but not panic
	r.processEvent(cfg, event)
}

func TestRunner_RunEventPipeline(t *testing.T) {
	b := bus.NewMemoryBus()
	received := make(chan []byte, 1)
	ms := &mockSinkWithChannel{received: received}
	r := NewRunner(b, map[string]sink.Sink{"test": ms})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	cfg := config.PipelineConfig{
		SourceTopic: "logs",
		Name:        "logger",
		Dispatch: &config.DispatchConfig{
			Sinks: []config.PipelineSinkConfig{
				{Name: "test"},
			},
		},
	}

	// Start pipeline in background
	go r.runEventPipeline(ctx, cfg)

	// We need to wait a tiny bit for the subscription to happen inside the goroutine
	time.Sleep(20 * time.Millisecond)

	// Publish an event to the bus
	err := b.Publish("logs", []byte("test data"))
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

// mockSinkWithChannel implements the sink.Sink interface for testing with a channel
type mockSinkWithChannel struct {
	received chan []byte
}

func (m *mockSinkWithChannel) Send(b []byte) error {
	m.received <- b
	return nil
}
