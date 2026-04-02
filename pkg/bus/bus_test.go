package bus

import (
	"bytes"
	"testing"
	"time"
)

func TestNewMemoryBus(t *testing.T) {
	bus := NewMemoryBus()
	if bus == nil {
		t.Fatal("expected NewMemoryBus to return a non-nil instance")
	}
	if bus.subscribers == nil {
		t.Error("expected subscribers map to be initialized")
	}
}

func TestMemoryBus_Subscribe(t *testing.T) {
	bus := NewMemoryBus()
	topic := "test-topic"
	ch := bus.Subscribe(topic)

	if ch == nil {
		t.Fatal("expected Subscribe to return a non-nil channel")
	}

	bus.lock.RLock()
	defer bus.lock.RUnlock()
	subs, ok := bus.subscribers[topic]
	if !ok {
		t.Errorf("expected topic %s to be registered in subscribers", topic)
	}
	if len(subs) != 1 {
		t.Errorf("expected 1 subscriber for topic %s, got %d", topic, len(subs))
	}
}

func TestMemoryBus_Publish(t *testing.T) {
	bus := NewMemoryBus()
	topic := "test-topic"
	payload := []byte("hello world")

	ch := bus.Subscribe(topic)

	err := bus.Publish(topic, payload)
	if err != nil {
		t.Fatalf("unexpected error on Publish: %v", err)
	}

	select {
	case event := <-ch:
		if event.Topic != topic {
			t.Errorf("expected topic %s, got %s", topic, event.Topic)
		}
		if !bytes.Equal(event.Payload, payload) {
			t.Errorf("expected payload %s, got %s", string(payload), string(event.Payload))
		}
		if event.ID != 1 {
			t.Errorf("expected event ID 1, got %d", event.ID)
		}
		if event.Mode != ModeVolatile {
			t.Errorf("expected mode ModeVolatile, got %v", event.Mode)
		}
		if event.Timestamp.IsZero() {
			t.Error("expected non-zero timestamp")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event")
	}
}

func TestMemoryBus_Publish_MultipleSubscribers(t *testing.T) {
	bus := NewMemoryBus()
	topic := "multi-topic"
	payload := []byte("multi data")

	ch1 := bus.Subscribe(topic)
	ch2 := bus.Subscribe(topic)

	err := bus.Publish(topic, payload)
	if err != nil {
		t.Fatalf("unexpected error on Publish: %v", err)
	}

	for i, ch := range []<-chan Event{ch1, ch2} {
		select {
		case event := <-ch:
			if !bytes.Equal(event.Payload, payload) {
				t.Errorf("subscriber %d: expected payload %s, got %s", i, string(payload), string(event.Payload))
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("subscriber %d timed out", i)
		}
	}
}

func TestMemoryBus_Publish_NoSubscribers(t *testing.T) {
	bus := NewMemoryBus()
	err := bus.Publish("no-one-cares", []byte("lonely data"))
	if err != nil {
		t.Errorf("Publish to topic with no subscribers should not error, got %v", err)
	}
}

func TestMemoryBus_Publish_Overflow(t *testing.T) {
	bus := NewMemoryBus()
	topic := "overflow"
	ch := bus.Subscribe(topic)

	// Buffer size is 1. Publish 2 events.
	for i := 0; i < 2; i++ {
		_ = bus.Publish(topic, []byte{byte(i)})
	}

	if len(ch) != 1 {
		t.Errorf("expected channel length 1, got %d", len(ch))
	}

	// The first event (byte(0)) should have been dropped.
	firstEvent := <-ch
	if firstEvent.Payload[0] != 1 {
		t.Errorf("expected first event payload to be 1 (0 should be dropped), got %d", firstEvent.Payload[0])
	}
}
