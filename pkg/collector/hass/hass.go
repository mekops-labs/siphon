package hass

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mitchellh/mapstructure"
)

type HassParams struct {
	Interval int `mapstructure:"interval"` // Polling interval in seconds
}

type hassSource struct {
	params   HassParams
	entities map[string]string // alias -> entity_id
	lock     sync.Mutex
	bus      bus.Bus
	client   *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Ensure hassSource implements the Collector interface
var _ collector.Collector = (*hassSource)(nil)

func init() {
	collector.Registry.Add("hass", New)
}

func New(p any) collector.Collector {
	var opt HassParams
	if err := mapstructure.Decode(p, &opt); err != nil {
		log.Printf("HASS collector config error: %v", err)
		return nil
	}

	if opt.Interval <= 0 {
		opt.Interval = 30 // Default to 30 seconds
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &hassSource{
		params:   opt,
		entities: make(map[string]string),
		client:   &http.Client{Timeout: 10 * time.Second},
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (h *hassSource) RegisterTopic(alias, topic string) {
	h.lock.Lock()
	defer h.lock.Unlock()
	// In the context of the HASS API, the "topic" is the Home Assistant entity_id
	h.entities[alias] = topic
	log.Printf("HASS Collector registered alias '%s' for entity '%s'", alias, topic)
}

func (h *hassSource) Start(b bus.Bus) {
	h.bus = b
	h.wg.Add(1)

	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(time.Duration(h.params.Interval) * time.Second)
		defer ticker.Stop()

		h.fetchStates() // Initial run

		for {
			select {
			case <-h.ctx.Done():
				return
			case <-ticker.C:
				h.fetchStates()
			}
		}
	}()
}

func (h *hassSource) fetchStates() {
	h.lock.Lock()
	defer h.lock.Unlock()

	// Grab the token automatically injected by the HA Supervisor
	token := os.Getenv("SUPERVISOR_TOKEN")
	if token == "" {
		log.Println("HASS Collector Warning: SUPERVISOR_TOKEN not found. Are you running inside Home Assistant?")
		return
	}

	for alias, entityID := range h.entities {
		var addr string
		// Use the internal Supervisor proxy to hit the Core API safely
		if entityID == "*" {
			addr = "http://supervisor/core/api/states"

		} else {
			// Ensure the entity ID is URL-encoded
			encodedEntityID := url.PathEscape(entityID)
			addr = fmt.Sprintf("http://supervisor/core/api/states/%s", encodedEntityID)
		}

		req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, addr, nil)
		if err != nil {
			continue
		}

		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := h.client.Do(req)
		if err != nil {
			log.Printf("HASS API Request failed [%s]: %v", entityID, err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err == nil && resp.StatusCode == 200 {
			// Publish the raw JSON response to the Event Bus using the ALIAS
			if err := h.bus.Publish(alias, body); err != nil {
				log.Printf("HASS Bus Publish Error [%s]: %v", alias, err)
			}
		} else {
			log.Printf("HASS API Error [%s]: HTTP %d", entityID, resp.StatusCode)
		}
	}
}

func (h *hassSource) End() {
	h.cancel()
	h.wg.Wait()
}
