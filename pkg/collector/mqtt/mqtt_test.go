package mqtt

import (
	"testing"
)

func TestMqttSource_New(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]interface{}
		isNil  bool
	}{
		{"ValidConfig", map[string]interface{}{"url": "tcp://localhost:1883"}, false},
		{"MissingUrl", map[string]interface{}{"user": "admin"}, true},
		{"InvalidParams", map[string]interface{}{"url": 123}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := New(tt.params)
			if (col == nil) != tt.isNil {
				t.Errorf("New() nil check failed: got %v, wantNil %v", col == nil, tt.isNil)
			}
		})
	}
}

func TestMqttSource_RegisterTopic(t *testing.T) {
	col := New(map[string]interface{}{"url": "tcp://localhost:1883"}).(*mqttSource)

	col.RegisterTopic("sensors/temp")
	col.RegisterTopic("sensors/hum")

	if len(col.topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(col.topics))
	}
}
