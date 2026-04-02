package jsonpath

import (
	"reflect"
	"testing"
)

func TestJsonpathParser_Parse(t *testing.T) {
	p := &jsonpathParser{}
	payload := []byte(`{"temperature": 22.5, "humidity": 45, "device": {"id": "sensor-01"}, "tags": ["living-room", "wifi"]}`)

	tests := []struct {
		name    string
		vars    map[string]string
		want    map[string]any
		wantErr bool
	}{
		{
			name: "Basic extraction",
			vars: map[string]string{
				"temp": "$.temperature",
				"h":    "$.humidity",
			},
			want: map[string]any{
				"temp": 22.5,
				"h":    float64(45), // json unmarshal defaults to float64 for numbers
			},
		},
		{
			name: "Nested and array extraction",
			vars: map[string]string{
				"dev_id": "$.device.id",
				"tag":    "$.tags[0]",
			},
			want: map[string]any{
				"dev_id": "sensor-01",
				"tag":    "living-room",
			},
		},
		{
			name: "Invalid path",
			vars: map[string]string{
				"missing": "$.non.existent",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(payload, tt.vars)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJsonpathParser_Parse_InvalidJSON(t *testing.T) {
	p := &jsonpathParser{}
	_, err := p.Parse([]byte(`{invalid}`), map[string]string{"v": "$.v"})
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
