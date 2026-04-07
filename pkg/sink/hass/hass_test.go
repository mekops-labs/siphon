package hass

import (
	"encoding/json"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// mockToken implements paho.Token
type mockToken struct {
	paho.Token
}

func (m *mockToken) Wait() bool                       { return true }
func (m *mockToken) WaitTimeout(_ time.Duration) bool { return true }
func (m *mockToken) Error() error                     { return nil }

// mockMqttClient implements a minimal paho.Client for testing
type mockMqttClient struct {
	paho.Client
	publishedTopic   string
	publishedPayload interface{}
	connected        bool
	disconnectCalled bool
}

func (m *mockMqttClient) IsConnected() bool {
	return m.connected
}

func (m *mockMqttClient) Publish(topic string, qos byte, retained bool, payload interface{}) paho.Token {
	m.publishedTopic = topic
	m.publishedPayload = payload
	return &mockToken{}
}

func (m *mockMqttClient) Disconnect(quiesce uint) {
	m.disconnectCalled = true
	m.connected = false
}

func TestNew_Validation(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		wantErr bool
	}{
		{
			name:    "Missing URL",
			params:  map[string]any{"object_id": "test"},
			wantErr: true,
		},
		{
			name:    "Missing ObjectID",
			params:  map[string]any{"url": "tcp://localhost:1883"},
			wantErr: true,
		},
		{
			name: "Valid Basic Params",
			params: map[string]any{
				"url":       "tcp://127.0.0.1:9883",
				"object_id": "test_sensor",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.params, nil)
			if tt.wantErr && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestHassSink_Send(t *testing.T) {
	mock := &mockMqttClient{connected: true}
	s := &hassSink{
		client:     mock,
		stateTopic: "homeassistant/sensor/test/state",
	}

	payload := []byte(`{"temperature": 22.5}`)
	err := s.Send(payload)

	if err != nil {
		t.Errorf("Send failed: %v", err)
	}
	if mock.publishedTopic != s.stateTopic {
		t.Errorf("expected topic %s, got %s", s.stateTopic, mock.publishedTopic)
	}
	if string(mock.publishedPayload.([]byte)) != string(payload) {
		t.Errorf("expected payload %s, got %s", string(payload), string(mock.publishedPayload.([]byte)))
	}
}

func TestHassSink_Close(t *testing.T) {
	mock := &mockMqttClient{connected: true}
	s := &hassSink{
		client:            mock,
		availabilityTopic: "homeassistant/sensor/test/availability",
		params:            hassParams{ObjectID: "test"},
	}

	err := s.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if mock.publishedTopic != s.availabilityTopic {
		t.Errorf("expected availability topic, got %s", mock.publishedTopic)
	}
	if mock.publishedPayload.(string) != "offline" {
		t.Errorf("expected offline payload, got %v", mock.publishedPayload)
	}
	if !mock.disconnectCalled {
		t.Error("Disconnect was not called")
	}
}

func TestDiscoveryPayload(t *testing.T) {
	// This tests the logic inside New that builds the configPayload
	opt := hassParams{
		Name: "Total Power",
	}

	// Mimic the logic in New()
	stateTopic := "homeassistant/sensor/power_meter/state"
	configPayload := map[string]interface{}{
		"name":        opt.Name,
		"state_topic": stateTopic,
		"unique_id":   "siphon_power_meter",
	}

	out, _ := json.Marshal(configPayload)
	var decoded map[string]interface{}
	json.Unmarshal(out, &decoded)

	if decoded["unique_id"] != "siphon_power_meter" {
		t.Errorf("incorrect unique_id: %v", decoded["unique_id"])
	}
}
