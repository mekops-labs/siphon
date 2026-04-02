package ntfy

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

const defaultTimeout = 5 * time.Second

type ntfyParams struct {
	URL      string `mapstructure:"url"`
	Topic    string `mapstructure:"topic"`
	Token    string `mapstructure:"token"`
	Title    string `mapstructure:"title"`
	Priority int    `mapstructure:"priority"`
}

type ntfySink struct {
	params ntfyParams
	client *http.Client
}

// Ensure ntfySink implements sink.Sink
var _ sink.Sink = (*ntfySink)(nil)

func init() {
	sink.Registry.Add("ntfy", New)
}

func New(params any) (sink.Sink, error) {
	var opt ntfyParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode ntfy sink params: %w", err)
	}

	if opt.URL == "" || opt.Topic == "" {
		return nil, fmt.Errorf("ntfy sink: url and topic are required fields")
	}

	if opt.Priority < 1 || opt.Priority > 5 {
		opt.Priority = 3
	}

	return &ntfySink{
		params: opt,
		client: &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (s *ntfySink) Send(b []byte) error {
	targetURL, err := url.JoinPath(s.params.URL, s.params.Topic)
	if err != nil {
		return fmt.Errorf("failed to construct target URL: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(b))
	if err != nil {
		return err
	}

	// Add headers based on config
	if s.params.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.params.Token)
	}
	if s.params.Title != "" {
		req.Header.Set("Title", s.params.Title)
	}
	if s.params.Priority > 0 {
		req.Header.Set("Priority", fmt.Sprintf("%d", s.params.Priority))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("ntfy request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ntfy returned non-2xx status: %d", resp.StatusCode)
	}

	return nil
}
