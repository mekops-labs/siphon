package rest

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mitchellh/mapstructure"
)

type RestParams struct {
	Interval int               `mapstructure:"interval"` // Polling interval in seconds
	Method   string            `mapstructure:"method"`   // HTTP Method (GET, POST, etc.)
	Headers  map[string]string `mapstructure:"headers"`  // Custom headers (e.g. Authorization)
	Body     string            `mapstructure:"body"`     // Optional request body
	Timeout  int               `mapstructure:"timeout"`  // Request timeout in seconds
}

type restSource struct {
	params RestParams
	urls   map[string]string // alias -> Target URL
	lock   sync.Mutex
	bus    bus.Bus
	client *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Ensure restSource implements the Collector interface
var _ collector.Collector = (*restSource)(nil)

func init() {
	collector.Registry.Add("rest", New)
}

func New(p any) collector.Collector {
	var opt RestParams
	if err := mapstructure.Decode(p, &opt); err != nil {
		log.Printf("REST collector config error: %v", err)
		return nil
	}

	// Apply sensible defaults
	if opt.Interval <= 0 {
		opt.Interval = 60
	}
	if opt.Method == "" {
		opt.Method = http.MethodGet
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 10
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &restSource{
		params: opt,
		urls:   make(map[string]string),
		client: &http.Client{Timeout: time.Duration(opt.Timeout) * time.Second},
		ctx:    ctx,
		cancel: cancel,
	}
}

func (r *restSource) RegisterTopic(alias, topic string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.urls[alias] = topic
}

func (r *restSource) Start(b bus.Bus) {
	r.bus = b
	r.wg.Add(1)

	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(time.Duration(r.params.Interval) * time.Second)
		defer ticker.Stop()

		r.fetchUrls() // Fire immediately on startup

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				r.fetchUrls()
			}
		}
	}()
}

func (r *restSource) fetchUrls() {
	r.lock.Lock()
	defer r.lock.Unlock()

	for alias, targetURL := range r.urls {
		var reqBody io.Reader
		if r.params.Body != "" {
			reqBody = bytes.NewBufferString(r.params.Body)
		}

		req, err := http.NewRequestWithContext(r.ctx, r.params.Method, targetURL, reqBody)
		if err != nil {
			log.Printf("REST Collector failed to build request [%s]: %v", alias, err)
			continue
		}

		// Inject custom headers
		for key, val := range r.params.Headers {
			req.Header.Set(key, val)
		}

		resp, err := r.client.Do(req)
		if err != nil {
			log.Printf("REST Collector request failed [%s]: %v", alias, err)
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Only publish successful responses to the Event Bus
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if err := r.bus.Publish(alias, bodyBytes); err != nil {
				log.Printf("REST Bus Publish Error [%s]: %v", alias, err)
			}
		} else {
			log.Printf("REST API Error [%s]: HTTP %d", targetURL, resp.StatusCode)
		}
	}
}

func (r *restSource) End() {
	r.cancel()
	r.wg.Wait()
	log.Println("REST Collector successfully stopped.")
}
