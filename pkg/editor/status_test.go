package editor

import (
	"sync"
	"testing"
)

func TestNewStatus(t *testing.T) {
	s := NewStatus()
	snap := s.Get()

	if snap.OK {
		t.Error("expected initial ok to be false")
	}
	if snap.Message != "Starting..." {
		t.Errorf("expected initial message 'Starting...', got %q", snap.Message)
	}
	if snap.PipelineCount != 0 {
		t.Errorf("expected initial pipeline_count 0, got %d", snap.PipelineCount)
	}
	if len(snap.Warnings) != 0 {
		t.Errorf("expected no initial warnings, got %v", snap.Warnings)
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

			snap := s.Get()
			if snap.OK != tt.ok {
				t.Errorf("expected ok=%v, got %v", tt.ok, snap.OK)
			}
			if snap.Message != tt.message {
				t.Errorf("expected message %q, got %q", tt.message, snap.Message)
			}
			if snap.PipelineCount != 0 {
				t.Errorf("Set should reset pipeline_count, got %d", snap.PipelineCount)
			}
			if snap.CollectorCount != 0 {
				t.Errorf("Set should reset collector_count, got %d", snap.CollectorCount)
			}
			if snap.SinkCount != 0 {
				t.Errorf("Set should reset sink_count, got %d", snap.SinkCount)
			}
			if len(snap.Warnings) != 0 {
				t.Errorf("Set should reset warnings, got %v", snap.Warnings)
			}
		})
	}
}

func TestStatus_SetOverwrites(t *testing.T) {
	s := NewStatus()

	s.Set(true, "Running")
	s.Set(false, "config error: bad yaml")

	snap := s.Get()
	if snap.OK {
		t.Error("expected ok to be false after second Set")
	}
	if snap.Message != "config error: bad yaml" {
		t.Errorf("unexpected message: %q", snap.Message)
	}
}

func TestStatus_SetRunning(t *testing.T) {
	s := NewStatus()
	warnings := []string{"sink \"foo\": unknown type \"bad\"", "pipeline \"p1\": sink not found: missing"}
	s.SetRunning(3, 2, 4, warnings)

	snap := s.Get()
	if !snap.OK {
		t.Error("expected ok=true after SetRunning")
	}
	if snap.Message != "Running" {
		t.Errorf("expected message 'Running', got %q", snap.Message)
	}
	if snap.PipelineCount != 3 {
		t.Errorf("expected pipeline_count=3, got %d", snap.PipelineCount)
	}
	if snap.CollectorCount != 2 {
		t.Errorf("expected collector_count=2, got %d", snap.CollectorCount)
	}
	if snap.SinkCount != 4 {
		t.Errorf("expected sink_count=4, got %d", snap.SinkCount)
	}
	if len(snap.Warnings) != 2 {
		t.Fatalf("expected 2 warnings, got %d", len(snap.Warnings))
	}
	if snap.Warnings[0] != warnings[0] || snap.Warnings[1] != warnings[1] {
		t.Errorf("warnings mismatch: got %v", snap.Warnings)
	}
}

func TestStatus_SetRunning_NoWarnings(t *testing.T) {
	s := NewStatus()
	s.SetRunning(5, 3, 2, nil)

	snap := s.Get()
	if !snap.OK {
		t.Error("expected ok=true")
	}
	if snap.PipelineCount != 5 {
		t.Errorf("expected pipeline_count=5, got %d", snap.PipelineCount)
	}
	if snap.CollectorCount != 3 {
		t.Errorf("expected collector_count=3, got %d", snap.CollectorCount)
	}
	if snap.SinkCount != 2 {
		t.Errorf("expected sink_count=2, got %d", snap.SinkCount)
	}
	if len(snap.Warnings) != 0 {
		t.Errorf("expected no warnings, got %v", snap.Warnings)
	}
}

func TestStatus_SetRunning_IsolatesSlice(t *testing.T) {
	// Mutating the original slice after SetRunning must not affect the stored copy.
	s := NewStatus()
	orig := []string{"w1"}
	s.SetRunning(1, 1, 1, orig)
	orig[0] = "mutated"

	snap := s.Get()
	if snap.Warnings[0] != "w1" {
		t.Errorf("SetRunning should copy the warnings slice, got %q", snap.Warnings[0])
	}

	// Mutating the returned snapshot slice must not affect the stored copy.
	snap.Warnings[0] = "mutated again"
	snap2 := s.Get()
	if snap2.Warnings[0] != "w1" {
		t.Errorf("Get should return a copy of the warnings slice, got %q", snap2.Warnings[0])
	}
}

func TestStatus_SetOverwritesSetRunning(t *testing.T) {
	s := NewStatus()
	s.SetRunning(4, 2, 3, []string{"some warning"})
	s.Set(false, "config error: parse failed")

	snap := s.Get()
	if snap.OK {
		t.Error("expected ok=false after Set overwrites SetRunning")
	}
	if snap.PipelineCount != 0 {
		t.Errorf("Set should reset pipeline_count, got %d", snap.PipelineCount)
	}
	if snap.CollectorCount != 0 {
		t.Errorf("Set should reset collector_count, got %d", snap.CollectorCount)
	}
	if snap.SinkCount != 0 {
		t.Errorf("Set should reset sink_count, got %d", snap.SinkCount)
	}
	if len(snap.Warnings) != 0 {
		t.Errorf("Set should reset warnings, got %v", snap.Warnings)
	}
}

func TestStatus_ConcurrentAccess(t *testing.T) {
	// Run with -race to detect data races.
	s := NewStatus()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			s.Set(i%2 == 0, "message")
		}(i)
		go func(i int) {
			defer wg.Done()
			s.SetRunning(i, i, i, []string{"w"})
		}(i)
		go func() {
			defer wg.Done()
			s.Get()
		}()
	}

	wg.Wait()
}
