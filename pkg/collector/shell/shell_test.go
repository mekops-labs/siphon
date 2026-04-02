package shell

import (
	"testing"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
)

type mockBus struct {
	published chan struct {
		topic   string
		payload []byte
	}
}

func (m *mockBus) Publish(topic string, payload []byte) error {
	m.published <- struct {
		topic   string
		payload []byte
	}{topic, payload}
	return nil
}

func (m *mockBus) Subscribe(topic string) <-chan bus.Event { return nil }

func TestShellSource(t *testing.T) {
	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 1)}

	params := map[string]interface{}{
		"interval": 1,
	}

	col := New(params)
	if col == nil {
		t.Fatal("failed to create collector")
	}

	cmd := "echo 'hello shell'"
	col.RegisterTopic(cmd)
	col.Start(b)
	defer col.End()

	select {
	case msg := <-b.published:
		if msg.topic != cmd {
			t.Errorf("expected topic %s, got %s", cmd, msg.topic)
		}
		// Shell output usually includes a newline
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for shell execution")
	}
}
