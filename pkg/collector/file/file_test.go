package file

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
)

type mockBus struct {
	published chan struct {
		topic   string
		payload []byte
	}
	publishErr error
}

func (m *mockBus) Publish(topic string, payload []byte) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published <- struct {
		topic   string
		payload []byte
	}{topic, payload}
	return nil
}

func (m *mockBus) Subscribe(topic string) <-chan bus.Event { return nil }

func TestFileSource(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello file")

	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 1)}

	params := map[string]interface{}{
		"interval": 1, // 1 second
	}

	col := New(params)
	if col == nil {
		t.Fatal("failed to create collector")
	}

	col.RegisterTopic("test", tmpFile)
	col.Start(b)
	defer col.End()

	select {
	case msg := <-b.published:
		if msg.topic != "test" {
			t.Errorf("expected topic %s, got %s", "test", msg.topic)
		}
		if string(msg.payload) != string(content) {
			t.Errorf("expected payload %s, got %s", string(content), string(msg.payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for file read")
	}
}

func TestFileSource_UnreadableFile(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "noperm.txt")
	if err := os.WriteFile(tmpFile, []byte("secret"), 0000); err != nil {
		t.Fatal(err)
	}

	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 1)}

	col := New(map[string]interface{}{"interval": 60})
	col.RegisterTopic("noperm", tmpFile)

	// readFiles should not panic; it logs and continues.
	fs := col.(*fileSource)
	fs.bus = b
	fs.readFiles() // direct call — no goroutine needed

	select {
	case msg := <-b.published:
		t.Errorf("expected no publish for unreadable file, got topic %s", msg.topic)
	default:
		// expected
	}
}

func TestFileSource_BusPublishError(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "data.txt")
	if err := os.WriteFile(tmpFile, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	b := &mockBus{
		published: make(chan struct {
			topic   string
			payload []byte
		}, 1),
		publishErr: fmt.Errorf("bus is full"),
	}

	col := New(map[string]interface{}{"interval": 60})
	col.RegisterTopic("data", tmpFile)

	fs := col.(*fileSource)
	fs.bus = b

	// Must not panic when Publish returns an error.
	fs.readFiles()
}
