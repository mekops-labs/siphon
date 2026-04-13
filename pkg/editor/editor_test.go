package editor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// setupConfig writes content to a temp file and returns its path.
func setupConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}

// ---- /api/config GET --------------------------------------------------------

func TestConfigGet_Success(t *testing.T) {
	content := "version: 2\n"
	path := setupConfig(t, content)

	reload := make(chan struct{}, 1)
	mux := newMux(path, reload, NewStatus())

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if got := w.Body.String(); got != content {
		t.Errorf("expected body %q, got %q", content, got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/yaml" {
		t.Errorf("expected Content-Type text/yaml, got %q", ct)
	}
}

func TestConfigGet_FileMissing(t *testing.T) {
	reload := make(chan struct{}, 1)
	mux := newMux("/nonexistent/path/config.yaml", reload, NewStatus())

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

// ---- /api/config POST -------------------------------------------------------

func TestConfigPost_Success(t *testing.T) {
	path := setupConfig(t, "version: 2\n")
	reload := make(chan struct{}, 1)
	mux := newMux(path, reload, NewStatus())

	newContent := "version: 2\n# updated\n"
	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(newContent))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "Saved" {
		t.Errorf("expected body 'Saved', got %q", w.Body.String())
	}

	// Verify the file was actually written.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read config after save: %v", err)
	}
	if string(data) != newContent {
		t.Errorf("file content mismatch: expected %q, got %q", newContent, string(data))
	}
}

func TestConfigPost_TriggersReload(t *testing.T) {
	path := setupConfig(t, "version: 2\n")
	reload := make(chan struct{}, 1)
	mux := newMux(path, reload, NewStatus())

	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader("version: 2\n"))
	mux.ServeHTTP(httptest.NewRecorder(), req)

	select {
	case <-reload:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Error("expected reload signal after save, got none")
	}
}

func TestConfigPost_DoesNotBlockWhenReloadPending(t *testing.T) {
	// reload channel already has a pending signal — second save must not block.
	path := setupConfig(t, "version: 2\n")
	reload := make(chan struct{}, 1)
	reload <- struct{}{} // pre-fill the buffer

	mux := newMux(path, reload, NewStatus())

	done := make(chan struct{})
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader("version: 2\n"))
		mux.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()

	select {
	case <-done:
		// expected — handler returned without blocking
	case <-time.After(500 * time.Millisecond):
		t.Error("handler blocked when reload channel was already full")
	}
}

func TestConfigPost_EmptyBody(t *testing.T) {
	path := setupConfig(t, "version: 2\n")
	reload := make(chan struct{}, 1)
	mux := newMux(path, reload, NewStatus())

	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(""))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty body, got %d", w.Code)
	}

	// No reload signal should be sent.
	select {
	case <-reload:
		t.Error("reload signal sent for empty body request")
	default:
	}
}

func TestConfigPost_ReadOnlyFile(t *testing.T) {
	path := setupConfig(t, "version: 2\n")
	// Make the file read-only so the write fails.
	os.Chmod(path, 0444)
	t.Cleanup(func() { os.Chmod(path, 0644) })

	reload := make(chan struct{}, 1)
	mux := newMux(path, reload, NewStatus())

	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader("version: 2\n"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for read-only file, got %d", w.Code)
	}

	// No reload signal on write failure.
	select {
	case <-reload:
		t.Error("reload signal sent despite write failure")
	default:
	}
}

// ---- /api/config method not allowed -----------------------------------------

func TestConfigEndpoint_MethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			path := setupConfig(t, "version: 2\n")
			mux := newMux(path, make(chan struct{}, 1), NewStatus())

			req := httptest.NewRequest(method, "/api/config", nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("expected 405 for %s, got %d", method, w.Code)
			}
		})
	}
}

// ---- /api/status ------------------------------------------------------------

func TestStatusEndpoint_OK(t *testing.T) {
	status := NewStatus()
	status.Set(true, "Running")
	mux := newMux("/any", make(chan struct{}, 1), status)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["ok"] != true {
		t.Errorf("expected ok=true, got %v", body["ok"])
	}
	if body["message"] != "Running" {
		t.Errorf("expected message='Running', got %v", body["message"])
	}
}

func TestStatusEndpoint_Error(t *testing.T) {
	status := NewStatus()
	status.Set(false, "config error: bad yaml")
	mux := newMux("/any", make(chan struct{}, 1), status)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)

	if body["ok"] != false {
		t.Errorf("expected ok=false, got %v", body["ok"])
	}
	if body["message"] != "config error: bad yaml" {
		t.Errorf("unexpected message: %v", body["message"])
	}
}

func TestStatusEndpoint_ContentType(t *testing.T) {
	mux := newMux("/any", make(chan struct{}, 1), NewStatus())

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
}

func TestStatusEndpoint_ReflectsUpdates(t *testing.T) {
	status := NewStatus()
	mux := newMux("/any", make(chan struct{}, 1), status)

	check := func(wantOK bool, wantMsg string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		var body map[string]any
		json.NewDecoder(w.Body).Decode(&body)

		if body["ok"] != wantOK {
			t.Errorf("expected ok=%v, got %v", wantOK, body["ok"])
		}
		if body["message"] != wantMsg {
			t.Errorf("expected message=%q, got %v", wantMsg, body["message"])
		}
	}

	check(false, "Starting...")

	status.Set(true, "Running")
	check(true, "Running")

	status.Set(false, "config error: parse failed")
	check(false, "config error: parse failed")
}
