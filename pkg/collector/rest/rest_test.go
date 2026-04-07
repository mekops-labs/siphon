package rest

import (
	"io"
	"net/http"
	"net/http/httptest"
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
		r := c.(*restSource)
		if r.params.Interval != 60 {
			t.Errorf("expected default interval 60, got %d", r.params.Interval)
		}
		if r.params.Method != http.MethodGet {
			t.Errorf("expected default method GET, got %s", r.params.Method)
		}
		if r.params.Timeout != 10 {
			t.Errorf("expected default timeout 10, got %d", r.params.Timeout)
		}
	})

	t.Run("CustomValues", func(t *testing.T) {
		params := map[string]any{
			"interval": 10,
			"method":   "POST",
			"timeout":  5,
			"headers":  map[string]string{"Auth": "token"},
			"body":     "ping",
		}
		c := New(params)
		r := c.(*restSource)
		if r.params.Interval != 10 || r.params.Method != "POST" || r.params.Timeout != 5 {
			t.Errorf("custom values not set correctly")
		}
		if r.params.Headers["Auth"] != "token" || r.params.Body != "ping" {
			t.Errorf("custom headers or body not set correctly")
		}
	})
}

func TestRegisterTopic(t *testing.T) {
	c := New(map[string]any{})
	r := c.(*restSource)
	r.RegisterTopic("my-alias", "http://example.com/api")

	r.lock.Lock()
	url, ok := r.urls["my-alias"]
	r.lock.Unlock()

	if !ok || url != "http://example.com/api" {
		t.Errorf("URL not registered correctly")
	}
}

func TestFetchUrls(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.Header.Get("X-Test") != "value" {
			t.Errorf("missing header")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "test-body" {
			t.Errorf("unexpected body: %s", string(body))
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("response-data"))
	}))
	defer ts.Close()

	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 1)}

	params := map[string]any{
		"method":  "POST",
		"headers": map[string]string{"X-Test": "value"},
		"body":    "test-body",
	}

	c := New(params)
	r := c.(*restSource)
	r.bus = b
	r.RegisterTopic("test-alias", ts.URL)

	r.fetchUrls()

	select {
	case msg := <-b.published:
		if msg.topic != "test-alias" {
			t.Errorf("expected topic test-alias, got %s", msg.topic)
		}
		if string(msg.payload) != "response-data" {
			t.Errorf("expected payload response-data, got %s", string(msg.payload))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for bus publish")
	}
}

func TestFetchUrls_Errors(t *testing.T) {
	t.Run("StatusError", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer ts.Close()

		b := &mockBus{published: make(chan struct {
			topic   string
			payload []byte
		}, 1)}

		c := New(map[string]any{})
		r := c.(*restSource)
		r.bus = b
		r.RegisterTopic("err", ts.URL)

		r.fetchUrls()

		select {
		case <-b.published:
			t.Error("should not publish on 404")
		case <-time.After(100 * time.Millisecond):
			// Success
		}
	})
}

func TestLifecycle(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := &mockBus{published: make(chan struct {
		topic   string
		payload []byte
	}, 2)}

	c := New(map[string]any{"interval": 1})
	r := c.(*restSource)
	r.RegisterTopic("t", ts.URL)

	r.Start(b)

	// Should publish immediately on Start
	select {
	case <-b.published:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("immediate fetch failed")
	}

	r.End()
}
