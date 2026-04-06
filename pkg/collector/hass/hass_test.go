package hass

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
)

// mockTransport allows us to intercept http.Client calls without a real server
type mockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

func TestNew(t *testing.T) {
	t.Run("DefaultInterval", func(t *testing.T) {
		c := New(map[string]any{})
		h := c.(*hassSource)
		if h.params.Interval != 30 {
			t.Errorf("expected default interval 30, got %d", h.params.Interval)
		}
	})

	t.Run("CustomInterval", func(t *testing.T) {
		c := New(map[string]any{"interval": 10})
		h := c.(*hassSource)
		if h.params.Interval != 10 {
			t.Errorf("expected interval 10, got %d", h.params.Interval)
		}
	})
}

func TestRegisterTopic(t *testing.T) {
	c := New(HassParams{Interval: 5})
	h := c.(*hassSource)
	h.RegisterTopic("temp", "sensor.living_room_temperature")

	h.lock.Lock()
	entity, ok := h.entities["temp"]
	h.lock.Unlock()

	if !ok || entity != "sensor.living_room_temperature" {
		t.Errorf("entity not registered correctly")
	}
}

func TestFetchStates(t *testing.T) {
	// Mock the supervisor environment
	os.Setenv("SUPERVISOR_TOKEN", "mock-token")
	defer os.Unsetenv("SUPERVISOR_TOKEN")

	b := bus.NewMemoryBus()
	c := New(HassParams{Interval: 1})
	h := c.(*hassSource)
	h.bus = b
	h.RegisterTopic("my_sensor", "sensor.test")

	mockJSON := `{"entity_id": "sensor.test", "state": "on"}`

	h.client.Transport = &mockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			expectedURL := "http://supervisor/core/api/states/sensor.test"
			if req.URL.String() != expectedURL {
				return nil, fmt.Errorf("unexpected URL: %s", req.URL.String())
			}
			if req.Header.Get("Authorization") != "Bearer mock-token" {
				return nil, fmt.Errorf("missing or invalid auth header")
			}

			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(mockJSON)),
			}, nil
		},
	}

	// Subscribe to the bus to verify output
	ch := b.Subscribe("my_sensor")

	h.fetchStates()

	select {
	case event := <-ch:
		if string(event.Payload) != mockJSON {
			t.Errorf("expected %s, got %s", mockJSON, string(event.Payload))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for bus event")
	}
}

func TestFetchStates_NoToken(t *testing.T) {
	os.Unsetenv("SUPERVISOR_TOKEN")

	b := bus.NewMemoryBus()
	c := New(HassParams{Interval: 1})
	h := c.(*hassSource)
	h.bus = b
	h.RegisterTopic("test", "sensor.test")

	// This should return early and not crash
	h.fetchStates()

	ch := b.Subscribe("test")
	select {
	case <-ch:
		t.Error("expected no events when token is missing")
	case <-time.After(50 * time.Millisecond):
		// Success
	}
}
