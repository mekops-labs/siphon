package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mitchellh/mapstructure"
)

type webhookParams struct {
	Port      int     `mapstructure:"port"`
	Token     string  `mapstructure:"token"`
	MaxBodyMB int64   `mapstructure:"max_body_mb"`
	RPS       float64 `mapstructure:"rps"`
	Burst     int     `mapstructure:"burst"`
	DedupeTTL int     `mapstructure:"dedupe_ttl"` // NEW: Time-to-live for duplicate detection in seconds
}

type webhookSource struct {
	params  webhookParams
	routes  map[string]string
	lock    sync.Mutex
	bus     bus.Bus
	limiter *rate.Limiter

	// NEW: Idempotency Cache State
	seenCache map[string]time.Time
	cacheLock sync.RWMutex

	server *http.Server
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Ensure webhookSource implements the Collector interface
var _ collector.Collector = (*webhookSource)(nil)

func init() {
	collector.Registry.Add("webhook", New)
}

func New(p any) collector.Collector {
	var opt webhookParams
	if err := mapstructure.Decode(p, &opt); err != nil {
		log.Printf("Webhook collector config error: %v", err)
		return nil
	}

	if opt.Port == 0 {
		opt.Port = 8080
	}
	if opt.MaxBodyMB <= 0 {
		opt.MaxBodyMB = 2
	}
	if opt.RPS <= 0 {
		opt.RPS = 10.0
	}
	if opt.Burst <= 0 {
		opt.Burst = 20
	}
	if opt.DedupeTTL <= 0 {
		opt.DedupeTTL = 300 // Default: Remember payloads for 5 minutes (300s)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &webhookSource{
		params:    opt,
		routes:    make(map[string]string),
		limiter:   rate.NewLimiter(rate.Limit(opt.RPS), opt.Burst),
		seenCache: make(map[string]time.Time),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (w *webhookSource) RegisterTopic(alias, topic string) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.routes[alias] = topic
}

func (w *webhookSource) Start(b bus.Bus) {
	w.bus = b
	w.wg.Add(1)

	// Start the background cache cleanup routine
	go w.cleanupCache()

	mux := http.NewServeMux()

	for alias, path := range w.routes {
		a := alias
		p := path

		mux.HandleFunc(p, func(rw http.ResponseWriter, req *http.Request) {
			if !w.limiter.Allow() {
				http.Error(rw, "429 Too Many Requests", http.StatusTooManyRequests)
				return
			}

			if req.Method != http.MethodPost && req.Method != http.MethodPut {
				http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			if w.params.Token != "" {
				authHeader := req.Header.Get("Authorization")
				if authHeader != "Bearer "+w.params.Token {
					http.Error(rw, "unauthorized", http.StatusUnauthorized)
					return
				}
			}

			maxBytes := w.params.MaxBodyMB * 1024 * 1024
			req.Body = http.MaxBytesReader(rw, req.Body, maxBytes)

			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(rw, "payload too large or malformed", http.StatusRequestEntityTooLarge)
				return
			}
			defer req.Body.Close()

			if len(body) == 0 {
				rw.WriteHeader(http.StatusOK)
				return
			}

			// ==========================================
			// SECURITY 6: Deduplication / Idempotency
			// ==========================================
			// 1. Hash the body
			hashBytes := sha256.Sum256(body)
			hashStr := hex.EncodeToString(hashBytes[:])

			// 2. Check if we have seen it recently
			w.cacheLock.RLock()
			_, seen := w.seenCache[hashStr]
			w.cacheLock.RUnlock()

			if seen {
				// We already processed this exact payload recently.
				// Return 200 OK so the sender stops retrying, but DO NOT publish to the bus.
				log.Printf("Webhook [%s]: Ignored duplicate payload (Hash: %s)", a, hashStr[:8])
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte("OK"))
				return
			}

			// 3. Mark it as seen
			w.cacheLock.Lock()
			w.seenCache[hashStr] = time.Now()
			w.cacheLock.Unlock()
			// ==========================================

			if err := w.bus.Publish(a, body); err != nil {
				log.Printf("Webhook Bus Publish Error [%s]: %v", a, err)
				http.Error(rw, "internal routing error", http.StatusInternalServerError)
				return
			}

			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte("OK"))
		})
	}

	w.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", w.params.Port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	go func() {
		defer w.wg.Done()
		log.Printf("Starting Webhook HTTP listener on port %d...", w.params.Port)
		if err := w.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Webhook listener error: %v", err)
		}
	}()
}

// cleanupCache periodically sweeps the seenCache and removes expired hashes
// to prevent the application from slowly running out of memory.
func (w *webhookSource) cleanupCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	ttlDuration := time.Duration(w.params.DedupeTTL) * time.Second

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			w.cacheLock.Lock()
			for hash, timestamp := range w.seenCache {
				if now.Sub(timestamp) > ttlDuration {
					delete(w.seenCache, hash)
				}
			}
			w.cacheLock.Unlock()
		}
	}
}

func (w *webhookSource) End() {
	w.cancel() // Stops the cache cleanup goroutine
	if w.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		w.server.Shutdown(ctx)
	}
	w.wg.Wait()
	log.Println("Webhook Collector successfully stopped.")
}
