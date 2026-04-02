package iotplotter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPlotterSink_Send(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v2/feed/test-feed" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("api-key") != "test-key" {
			t.Errorf("unexpected api-key: %s", r.Header.Get("api-key"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content-type: %s", r.Header.Get("Content-Type"))
		}

		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"data": 42}` {
			t.Errorf("unexpected body: %s", string(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	params := map[string]interface{}{
		"url":    ts.URL,
		"apikey": "test-key",
		"feed":   "test-feed",
	}

	s, err := New(params)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Send([]byte(`{"data": 42}`))
	if err != nil {
		t.Error(err)
	}
}

func TestPlotterSink_New_Validation(t *testing.T) {
	params := map[string]interface{}{"apikey": "k"} // missing feed
	_, err := New(params)
	if err == nil {
		t.Error("expected error when feed is missing")
	}
}
