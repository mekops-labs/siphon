package windy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWindySink_Send(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/test-api-key" {
			t.Errorf("expected path /test-api-key, got %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var payload map[string]interface{}
		json.Unmarshal(body, &payload)

		if payload["temp"] != 22.5 {
			t.Errorf("expected temp 22.5, got %v", payload["temp"])
		}
		if int(payload["station"].(float64)) != 123 {
			t.Errorf("expected station 123, got %v", payload["station"])
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Override for testing
	oldURL := windyAPIURL
	windyAPIURL = ts.URL + "/"
	defer func() { windyAPIURL = oldURL }()

	params := map[string]interface{}{
		"apikey": "test-api-key",
		"id":     123,
	}

	s, err := New(params)
	if err != nil {
		t.Fatal(err)
	}

	input := []byte(`{"temp": 22.5}`)
	if err := s.Send(input); err != nil {
		t.Fatal(err)
	}
}

func TestWindySink_New_Validation(t *testing.T) {
	if _, err := New(map[string]any{}); err == nil {
		t.Error("expected error for missing apikey")
	}
}
