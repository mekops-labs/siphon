package ntfy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNtfySink_Send(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/test-topic" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("unexpected auth: %s", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Title") != "Alert" {
			t.Errorf("unexpected title: %s", r.Header.Get("Title"))
		}
		if r.Header.Get("Priority") != "4" {
			t.Errorf("unexpected priority: %s", r.Header.Get("Priority"))
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "message body" {
			t.Errorf("unexpected body: %s", string(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	params := map[string]interface{}{
		"url":      ts.URL,
		"topic":    "test-topic",
		"token":    "test-token",
		"title":    "Alert",
		"priority": 4,
	}

	s, err := New(params, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Send([]byte("message body"))
	if err != nil {
		t.Error(err)
	}
}

func TestNtfySink_New_Validation(t *testing.T) {
	tests := []struct {
		params map[string]any
	}{
		{map[string]any{"url": "u"}},
		{map[string]any{"topic": "t"}},
	}
	for _, tt := range tests {
		if _, err := New(tt.params, nil); err == nil {
			t.Errorf("expected error for params: %v", tt.params)
		}
	}
}
