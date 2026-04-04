package stdout

import "testing"

func TestStdoutSink_Send(t *testing.T) {
	s, _ := New(nil, nil)
	err := s.Send([]byte("test output"))
	if err != nil {
		t.Errorf("stdout sink failed: %v", err)
	}
}
