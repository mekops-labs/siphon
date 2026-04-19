package ntfy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

func TestNtfySink_ReformatTitle(t *testing.T) {
	t.Run("PlainString", func(t *testing.T) {
		out, err := reformatTitle("Hello")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != "Hello" {
			t.Errorf("expected 'Hello', got %q", out)
		}
	})

	t.Run("NowTemplate", func(t *testing.T) {
		year := time.Now().Format("2006")
		out, err := reformatTitle(`{{now "2006"}}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != year {
			t.Errorf("expected year %q, got %q", year, out)
		}
	})

	t.Run("InvalidSyntax", func(t *testing.T) {
		_, err := reformatTitle("{{invalid syntax")
		if err == nil {
			t.Error("expected error for invalid template syntax")
		}
	})
}

func TestNtfySink_Send_Non2xxStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer ts.Close()

	s, _ := New(map[string]any{"url": ts.URL, "topic": "t"}, nil)
	err := s.Send([]byte("msg"))
	if err == nil {
		t.Error("expected error for non-2xx status")
	}
}

func TestNtfySink_Send_TransportError(t *testing.T) {
	s, _ := New(map[string]any{"url": "http://127.0.0.1:1", "topic": "t"}, nil)
	err := s.Send([]byte("msg"))
	if err == nil {
		t.Error("expected error for transport failure")
	}
}

func TestNtfySink_Send_NoTitleHeader(t *testing.T) {
	var titleHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		titleHeader = r.Header.Get("Title")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s, _ := New(map[string]any{"url": ts.URL, "topic": "t"}, nil)
	if err := s.Send([]byte("msg")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if titleHeader != "" {
		t.Errorf("expected no Title header when title is empty, got %q", titleHeader)
	}
}
