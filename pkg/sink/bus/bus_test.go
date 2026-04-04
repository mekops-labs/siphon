package bus

import (
	"testing"
	"time"

	eventBus "github.com/mekops-labs/siphon/pkg/bus"
)

func TestBusSink_Send(t *testing.T) {
	// Setup the memory bus
	eb := eventBus.NewMemoryBus()
	topic := "feedback-loop"

	// Initialize the sink
	params := map[string]any{
		"topic": topic,
	}
	s, err := New(params, eb)
	if err != nil {
		t.Fatalf("failed to create bus sink: %v", err)
	}

	// Subscribe to the target topic to verify delivery
	ch := eb.Subscribe(topic)

	// Send data
	payload := []byte("hello back")
	if err := s.Send(payload); err != nil {
		t.Errorf("Send failed: %v", err)
	}

	// Verify receipt
	select {
	case event := <-ch:
		if string(event.Payload) != string(payload) {
			t.Errorf("expected payload %s, got %s", string(payload), string(event.Payload))
		}
		if event.Topic != topic {
			t.Errorf("expected topic %s, got %s", topic, event.Topic)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for bus sink to publish")
	}
}

func TestBusSink_New_Validation(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		wantErr bool
	}{
		{"ValidTopic", map[string]any{"topic": "alerts"}, false},
		{"MissingTopic", map[string]any{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.params, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("%s: New() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			}
		})
	}
}
