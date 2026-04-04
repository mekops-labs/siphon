package iotplotter

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

const (
	defaultTimeout = 10 * time.Second
	defaultURL     = "https://iotplotter.com"
)

type plotterParams struct {
	URL    string `mapstructure:"url"`
	ApiKey string `mapstructure:"apikey"`
	Feed   string `mapstructure:"feed"`
}

type plotterSink struct {
	params plotterParams
	client *http.Client
}

// Ensure plotterSink implements sink.Sink
var _ sink.Sink = (*plotterSink)(nil)

func init() {
	sink.Registry.Add("iotplotter", New)
}

func New(params any, _ bus.Bus) (sink.Sink, error) {
	var opt plotterParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode iotplotter params: %w", err)
	}

	if opt.URL == "" {
		opt.URL = defaultURL
	}
	if opt.ApiKey == "" || opt.Feed == "" {
		return nil, fmt.Errorf("iotplotter requires both apikey and feed")
	}

	return &plotterSink{
		params: opt,
		client: &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (s *plotterSink) Send(b []byte) error {
	// IoTPlotter v2 API endpoint format
	targetURL, err := url.JoinPath(s.params.URL, "api", "v2", "feed", s.params.Feed)
	if err != nil {
		return fmt.Errorf("failed to construct target URL: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", s.params.ApiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("iotplotter request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("iotplotter returned non-2xx status: %d", resp.StatusCode)
	}

	return nil
}
