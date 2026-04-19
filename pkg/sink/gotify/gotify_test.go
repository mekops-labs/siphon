package gotify

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGotifySink_Send(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.URL.Path != "/message" {
			t.Errorf("expected path /message, got %s", r.URL.Path)
		}
		if r.Header.Get("X-Gotify-Key") != "test-token" {
			t.Errorf("expected token test-token, got %s", r.Header.Get("X-Gotify-Key"))
		}

		body, _ := io.ReadAll(r.Body)
		var payload map[string]interface{}
		json.Unmarshal(body, &payload)

		if payload["message"] != "hello world" {
			t.Errorf("expected message hello world, got %v", payload["message"])
		}
		if payload["title"] != "Test Title" {
			t.Errorf("expected title Test Title, got %v", payload["title"])
		}
		if int(payload["priority"].(float64)) != 5 {
			t.Errorf("expected priority 5, got %v", payload["priority"])
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	params := map[string]interface{}{
		"url":      ts.URL,
		"token":    "test-token",
		"title":    "Test Title",
		"priority": 5,
	}

	s, err := New(params, nil)
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}

	err = s.Send([]byte("hello world"))
	if err != nil {
		t.Errorf("Send failed: %v", err)
	}
}

func TestGotifySink_New_Errors(t *testing.T) {
	_, err := New(map[string]interface{}{"url": "http://localhost"}, nil)
	if err == nil {
		t.Error("expected error when token is missing")
	}

	_, err = New(map[string]interface{}{"token": "tok"}, nil)
	if err == nil {
		t.Error("expected error when url is missing")
	}
}

func TestGotifySink_ReformatTitle(t *testing.T) {
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

func TestGotifySink_Send_NonOKStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s, _ := New(map[string]interface{}{"url": ts.URL, "token": "tok"}, nil)
	err := s.Send([]byte("msg"))
	if err == nil {
		t.Error("expected error for non-200 status")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention status 500, got: %v", err)
	}
}

func TestGotifySink_Send_TransportError(t *testing.T) {
	s, _ := New(map[string]interface{}{"url": "http://127.0.0.1:1", "token": "tok"}, nil)
	err := s.Send([]byte("msg"))
	if err == nil {
		t.Error("expected error for transport failure")
	}
}

func TestGotifySink_Send_EmptyTitle(t *testing.T) {
	var received map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s, _ := New(map[string]interface{}{"url": ts.URL, "token": "tok"}, nil)
	if err := s.Send([]byte("msg")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received["title"] != "Siphon Alert" {
		t.Errorf("expected default title 'Siphon Alert', got %v", received["title"])
	}
}

func TestGotifySink_Send_TitleTemplateApplied(t *testing.T) {
	year := time.Now().Format("2006")
	var received map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s, _ := New(map[string]interface{}{"url": ts.URL, "token": "tok", "title": `Alert {{now "2006"}}`}, nil)
	if err := s.Send([]byte("msg")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "Alert " + year
	if received["title"] != expected {
		t.Errorf("expected title %q, got %v", expected, received["title"])
	}
}
