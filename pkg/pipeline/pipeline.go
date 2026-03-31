package pipeline

import (
	"context"
	"log"

	"github.com/mekops-labs/siphon/internal/config"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
)

type Runner struct {
	bus   bus.Bus
	sinks map[string]sink.Sink
}

func NewRunner(b bus.Bus, sinks map[string]sink.Sink) *Runner {
	return &Runner{
		bus:   b,
		sinks: sinks,
	}
}

// Start initializes all configured pipelines
func (r *Runner) Start(ctx context.Context, pipelines []config.PipelineConfig) {
	for _, pCfg := range pipelines {
		if pCfg.Type == "cron" {
			log.Printf("Pipeline [%s]: Cron dispatcher initialized (waiting for events)", pCfg.Name)
			// TODO: Implement cron state caching logic here
			continue
		}

		// Event-driven pipeline
		log.Printf("Pipeline [%s]: Subscribing to topic '%s'", pCfg.Name, pCfg.SourceTopic)
		go r.runEventPipeline(ctx, pCfg)
	}
}

func (r *Runner) runEventPipeline(ctx context.Context, cfg config.PipelineConfig) {
	ch := r.bus.Subscribe(cfg.SourceTopic)

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			r.processEvent(cfg, event)
		}
	}
}

func (r *Runner) processEvent(cfg config.PipelineConfig, event bus.Event) {
	// 1. TODO: Pass event.Payload through cfg.Parser
	// 2. TODO: Pass parsed data through cfg.Transform (expr engine)

	// For this initial step, we will forward the raw payload directly to the Sink
	// so you can verify data is flowing end-to-end.

	if cfg.Dispatch == nil {
		return
	}

	for _, sCfg := range cfg.Dispatch.Sinks {
		targetSink, ok := r.sinks[sCfg.Name]
		if !ok {
			log.Printf("Error: Sink '%s' not found for pipeline '%s'", sCfg.Name, cfg.Name)
			continue
		}

		// Eventually, this will take the Formatted output (template/expr).
		// For now, we simulate a successful pass-through.
		log.Printf("Pipeline [%s] -> Routing data to Sink [%s]", cfg.Name, sCfg.Name)

		// Note: You will need to update your Sink interface to accept standard data.
		// e.g., targetSink.Send(ctx, event.Payload)
		_ = targetSink
	}

	// 3. Acknowledge successful processing
	event.Ack()
}
