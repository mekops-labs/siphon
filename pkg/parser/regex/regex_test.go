package regex

import (
	"reflect"
	"testing"
)

func TestRegexParser_Parse(t *testing.T) {
	p := &regexParser{}
	payload := []byte(`ID: 12345, Status: ONLINE, Temperature: 21.5C, IP: 192.168.1.1`)

	tests := []struct {
		name    string
		vars    map[string]string
		want    map[string]any
		wantErr bool
	}{
		{
			name: "Basic matching",
			vars: map[string]string{
				"id":     `ID: (\d{5})`,
				"status": `ONLINE|OFFLINE`,
				"temp":   `Temperature: ([0-9.]+)C`,
				"ip":     `IP: (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`,
			},
			want: map[string]any{
				"id":     "12345",
				"status": "ONLINE",
				"temp":   "21.5",
				"ip":     "192.168.1.1",
			},
		},
		{
			name: "No match returns empty string",
			vars: map[string]string{
				"missing": `Missing: \d+`,
			},
			want: map[string]any{
				"missing": "",
			},
		},
		{
			name: "Invalid regex error",
			vars: map[string]string{
				"err": `(unclosed-group`,
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
