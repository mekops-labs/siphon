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
	col.RegisterTopic("test", cmd)
	col.Start(b)
	defer col.End()

	select {
	case msg := <-b.published:
		if msg.topic != "test" {
			t.Errorf("expected topic %s, got %s", "test", msg.topic)
		}
		if string(msg.payload) != "hello shell\n" {
			t.Errorf("expected payload %s, got %s", "hello shell", string(msg.payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for shell execution")
	}
}

func TestShellSource_NonZeroExit(t *testing.T) {
	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 1)}

	col := New(map[string]interface{}{"interval": 60})
	col.RegisterTopic("fail", "exit 1")

	ss := col.(*shellSource)
	ss.bus = b

	// Must not panic for a command that exits with a non-zero status.
	ss.executeCommands()

	select {
	case msg := <-b.published:
		t.Errorf("expected no publish for failed command, got topic %s", msg.topic)
	default:
		// expected
	}
}

func TestShellSource_InvalidCommand(t *testing.T) {
	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 1)}

	col := New(map[string]interface{}{"interval": 60})
	col.RegisterTopic("bad", "__no_such_command_xyz__")

	ss := col.(*shellSource)
	ss.bus = b

	// Must not panic for a command that cannot be found.
	ss.executeCommands()

	select {
	case msg := <-b.published:
		t.Errorf("expected no publish for invalid command, got topic %s", msg.topic)
	default:
		// expected
	}
}
