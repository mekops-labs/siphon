package windy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestWindySink_Send(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-password" {
			t.Errorf("expected Authorization header, got %s", r.Header.Get("Authorization"))
		}
		if r.URL.RawQuery != "station=123&temp=22.5" {
			t.Errorf("expected query ?station=123&temp=22.5, got %s", r.URL.RawQuery)
		}

		body, _ := io.ReadAll(r.Body)
		if string(body) != "" {
			t.Errorf("expected empty body, got %s", string(body))
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Override for testing
	oldURL := windyHost
	windyProto = "http"

	windyURL, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	windyHost = windyURL.Host
	windyPath = windyURL.Path
	defer func() { windyHost = oldURL; windyProto = "https" }()

	params := map[string]interface{}{
		"password": "test-password",
		"id":       123,
	}

	s, err := New(params, nil)
	if err != nil {
		t.Fatal(err)
	}

	input := []byte(`{"temp": 22.5}`)
	if err := s.Send(input); err != nil {
		t.Fatal(err)
	}
}

func TestWindySink_New_Validation(t *testing.T) {
	if _, err := New(map[string]any{}, nil); err == nil {
		t.Error("expected error for missing apikey")
	}
}
