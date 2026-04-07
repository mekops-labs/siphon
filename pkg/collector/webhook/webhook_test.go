package webhook

import (
	"bytes"
	"net/http"
	"strings"
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

func TestNew(t *testing.T) {
	t.Run("DefaultValues", func(t *testing.T) {
		c := New(map[string]any{})
		w := c.(*webhookSource)
		if w.params.Port != 8080 {
			t.Errorf("expected default port 8080, got %d", w.params.Port)
		}
		if w.params.MaxBodyMB != 2 {
			t.Errorf("expected default max_body_mb 2, got %d", w.params.MaxBodyMB)
		}
		if w.params.DedupeTTL != 300 {
			t.Errorf("expected default dedupe_ttl 300, got %d", w.params.DedupeTTL)
		}
		if w.params.RPS != 10.0 {
			t.Errorf("expected default RPS 10.0, got %f", w.params.RPS)
		}
	})

	t.Run("CustomValues", func(t *testing.T) {
		params := map[string]any{
			"port":        9090,
			"token":       "secret-token",
			"max_body_mb": 5,
			"rps":         5.0,
			"burst":       10,
			"dedupe_ttl":  60,
		}
		c := New(params)
		w := c.(*webhookSource)
		if w.params.Port != 9090 || w.params.Token != "secret-token" || w.params.MaxBodyMB != 5 {
			t.Errorf("custom values not set correctly")
		}
	})
}

func TestWebhookCollector_Integration(t *testing.T) {
	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 10)}

	// Use a non-standard port for integration testing
	port := 18081
	params := map[string]any{
		"port":        port,
		"token":       "secret",
		"max_body_mb": 1,
		"dedupe_ttl":  2,
	}
	c := New(params)
	w := c.(*webhookSource)
	w.RegisterTopic("test-alias", "/api/v1/push")

	w.Start(b)
	defer w.End()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	baseURL := "http://localhost:18081/api/v1/push"

	t.Run("SuccessfulPost", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, baseURL, strings.NewReader("payload1"))
		req.Header.Set("Authorization", "Bearer secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK, got %d", resp.StatusCode)
		}

		select {
		case msg := <-b.published:
			if msg.topic != "test-alias" || string(msg.payload) != "payload1" {
				t.Errorf("unexpected bus message: %+v", msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("timeout waiting for bus publish")
		}
	})

	t.Run("Deduplication", func(t *testing.T) {
		// Send payload1 again immediately
		req, _ := http.NewRequest(http.MethodPost, baseURL, strings.NewReader("payload1"))
		req.Header.Set("Authorization", "Bearer secret")
		resp, _ := http.DefaultClient.Do(req)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK for duplicate, got %d", resp.StatusCode)
		}

		// Nothing should be published to the bus for the duplicate
		select {
		case msg := <-b.published:
			t.Errorf("duplicate message published to bus: %s", string(msg.payload))
		case <-time.After(100 * time.Millisecond):
			// Success - nothing received
		}
	})

	t.Run("Unauthorized", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, baseURL, strings.NewReader("unauth"))
		resp, _ := http.DefaultClient.Do(req)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", resp.StatusCode)
		}
	})

	t.Run("PayloadTooLarge", func(t *testing.T) {
		// 1MB limit is set in params. Send a payload slightly larger than 1MB.
		largePayload := make([]byte, 1*1024*1024+1024)
		req, _ := http.NewRequest(http.MethodPost, baseURL, bytes.NewReader(largePayload))
		req.Header.Set("Authorization", "Bearer secret")
		resp, _ := http.DefaultClient.Do(req)
		defer resp.Body.Close()

		// http.MaxBytesReader results in a 413 Request Entity Too Large
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("expected 413 Request Entity Too Large, got %d", resp.StatusCode)
		}
	})
}
