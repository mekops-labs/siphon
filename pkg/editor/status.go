package editor

import "sync"

// Status holds the current engine state, safe for concurrent use.
type Status struct {
	mu      sync.RWMutex
	ok      bool
	message string
}

func NewStatus() *Status {
	return &Status{ok: false, message: "Starting..."}
}

func (s *Status) Set(ok bool, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ok = ok
	s.message = message
}

func (s *Status) Get() (ok bool, message string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ok, s.message
}
