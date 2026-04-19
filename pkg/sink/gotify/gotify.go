package gotify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

const defaultTimeout = 5 * time.Second

type gotifyParams struct {
	URL      string `mapstructure:"url"`
	Token    string `mapstructure:"token"`
	Title    string `mapstructure:"title"`
	Priority int    `mapstructure:"priority"`
}

type gotifySink struct {
	params gotifyParams
	client *http.Client
}

var _ sink.Sink = (*gotifySink)(nil)

func init() {
	sink.Registry.Add("gotify", New)
}

func reformatTitle(in string) (string, error) {
	fMap := template.FuncMap{
		"now": func(f string) string { return time.Now().Format(f) },
	}

	tmpl, err := template.New("title").Funcs(fMap).Parse(in)
	if err != nil {
		return "", err
	}

	var buf string
	title := bytes.NewBufferString(buf)

	// Run the template to verify the output.
	err = tmpl.Execute(title, nil)
	if err != nil {
		return "", err
	}

	return title.String(), nil
}

func New(params any, _ bus.Bus) (sink.Sink, error) {
	var opt gotifyParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode gotify params: %w", err)
	}

	if opt.URL == "" || opt.Token == "" {
		return nil, fmt.Errorf("gotify sink: url and token are required fields")
	}

	return &gotifySink{
		params: opt,
		client: &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (s *gotifySink) Send(b []byte) error {
	// Wrap the incoming bytes (the message) into Gotify's expected JSON format
	payload := map[string]interface{}{
		"message":  string(b),
		"title":    s.params.Title,
		"priority": s.params.Priority,
	}

	title := s.params.Title
	if title == "" {
		title = "Siphon Alert"
	} else {
		if formatted, err := reformatTitle(title); err != nil {
			log.Printf("Failed to reformat gotify title: %v", err)
		} else {
			title = formatted
		}
	}
	payload["title"] = title

	jsonPayload, _ := json.Marshal(payload)
	targetURL, err := url.JoinPath(s.params.URL, "message")
	if err != nil {
		return fmt.Errorf("failed to construct target URL: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(jsonPayload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Gotify-Key", s.params.Token)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("gotify request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gotify returned non-200 status: %d", resp.StatusCode)
	}

	return nil
}

func (s *gotifySink) Close() error { // No persistent connections to close for the gotify sink
	return nil
}
