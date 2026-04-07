package mqtt

import (
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// mockToken implements paho.Token for testing
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
	qos              byte
	retained         bool
	connected        bool
	disconnectCalled bool
}

func (m *mockMqttClient) IsConnected() bool {
	return m.connected
}

func (m *mockMqttClient) Publish(topic string, qos byte, retained bool, payload interface{}) paho.Token {
	m.publishedTopic = topic
	m.publishedPayload = payload
	m.qos = qos
	m.retained = retained
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
			params:  map[string]any{"topic": "test"},
			wantErr: true,
		},
		{
			name:    "Missing Topic",
			params:  map[string]any{"url": "tcp://localhost:1883"},
			wantErr: true,
		},
		{
			name: "Unreachable URL",
			params: map[string]any{
				"url":   "tcp://127.0.0.1:9883",
				"topic": "test",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.params, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMqttSink_Send(t *testing.T) {
	mock := &mockMqttClient{connected: true}
	s := &mqttSink{
		client: mock,
		params: mqttParams{
			Topic:    "sensors/temp",
			QoS:      1,
			Retained: true,
		},
	}

	payload := []byte("22.5")
	err := s.Send(payload)

	if err != nil {
		t.Errorf("Send() failed: %v", err)
	}
	if mock.publishedTopic != "sensors/temp" {
		t.Errorf("expected topic sensors/temp, got %s", mock.publishedTopic)
	}
	if mock.qos != 1 {
		t.Errorf("expected qos 1, got %d", mock.qos)
	}
	if !mock.retained {
		t.Error("expected retained to be true")
	}
	if string(mock.publishedPayload.([]byte)) != "22.5" {
		t.Errorf("expected payload 22.5, got %s", string(mock.publishedPayload.([]byte)))
	}
}

func TestMqttSink_Close(t *testing.T) {
	mock := &mockMqttClient{connected: true}
	s := &mqttSink{
		client: mock,
		params: mqttParams{URL: "tcp://localhost:1883"},
	}

	err := s.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	if !mock.disconnectCalled {
		t.Error("Disconnect() was not called")
	}
}
