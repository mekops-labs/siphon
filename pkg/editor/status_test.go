package editor

import (
	"sync"
	"testing"
)

func TestNewStatus(t *testing.T) {
	s := NewStatus()
	ok, msg := s.Get()

	if ok {
		t.Error("expected initial ok to be false")
	}
	if msg != "Starting..." {
		t.Errorf("expected initial message 'Starting...', got %q", msg)
	}
}

func TestStatus_Set(t *testing.T) {
	tests := []struct {
		name    string
		ok      bool
		message string
	}{
		{"Running", true, "Running"},
		{"ConfigError", false, "config error: failed to parse yaml"},
		{"EmptyMessage", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStatus()
			s.Set(tt.ok, tt.message)

			ok, msg := s.Get()
			if ok != tt.ok {
				t.Errorf("expected ok=%v, got %v", tt.ok, ok)
			}
			if msg != tt.message {
				t.Errorf("expected message %q, got %q", tt.message, msg)
			}
		})
	}
}

func TestStatus_SetOverwrites(t *testing.T) {
	s := NewStatus()

	s.Set(true, "Running")
	s.Set(false, "config error: bad yaml")

	ok, msg := s.Get()
	if ok {
		t.Error("expected ok to be false after second Set")
	}
	if msg != "config error: bad yaml" {
		t.Errorf("unexpected message: %q", msg)
	}
}

func TestStatus_ConcurrentAccess(t *testing.T) {
	// Run with -race to detect data races.
	s := NewStatus()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.Set(i%2 == 0, "message")
		}(i)
		go func() {
			defer wg.Done()
			s.Get()
		}()
	}

	wg.Wait()
}
