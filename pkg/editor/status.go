package editor

import "sync"

// Snapshot is an immutable copy of the engine status fields.
type Snapshot struct {
	OK             bool
	Message        string
	PipelineCount  int
	CollectorCount int
	SinkCount      int
	Warnings       []string
}

// Status holds the current engine state, safe for concurrent use.
type Status struct {
	mu             sync.RWMutex
	ok             bool
	message        string
	pipelineCount  int
	collectorCount int
	sinkCount      int
	warnings       []string
}

func NewStatus() *Status {
	return &Status{ok: false, message: "Starting..."}
}

// Set records a simple ok/error state and clears all component details.
// Use SetRunning when the engine started successfully.
func (s *Status) Set(ok bool, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ok = ok
	s.message = message
	s.pipelineCount = 0
	s.collectorCount = 0
	s.sinkCount = 0
	s.warnings = nil
}

// SetRunning marks the engine as running with component counts and any
// non-fatal warnings collected during startup (e.g. a sink that failed
// to initialise, or a pipeline that failed to compile).
func (s *Status) SetRunning(pipelineCount, collectorCount, sinkCount int, warnings []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ok = true
	s.message = "Running"
	s.pipelineCount = pipelineCount
	s.collectorCount = collectorCount
	s.sinkCount = sinkCount
	w := make([]string, len(warnings))
	copy(w, warnings)
	s.warnings = w
}

// Get returns an immutable snapshot of the current status.
func (s *Status) Get() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w := make([]string, len(s.warnings))
	copy(w, s.warnings)
	return Snapshot{
		OK:             s.ok,
		Message:        s.message,
		PipelineCount:  s.pipelineCount,
		CollectorCount: s.collectorCount,
		SinkCount:      s.sinkCount,
		Warnings:       w,
	}
}
