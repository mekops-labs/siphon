package gotify

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
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
