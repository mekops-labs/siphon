package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mekops-labs/siphon/internal/config"
	_ "github.com/mekops-labs/siphon/internal/modules"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mekops-labs/siphon/pkg/pipeline"
	"github.com/mekops-labs/siphon/pkg/sink"
)

var version = "unknown"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("need config file!")
	}

	log.Print("Starting Siphon v", version)

	// 1. Load Config
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	// 2. Initialize the Event Bus
	eventBus := bus.NewMemoryBus()

	// 2. Initialize Collectors
	collectors := make(map[string]collector.Collector)
	for name, colCfg := range cfg.Collectors {
		collectorInit, ok := collector.Registry[colCfg.Type]
		if !ok {
			log.Printf("unknown collector type: %s", colCfg.Type)
			continue
		}
		collectors[name] = collectorInit(colCfg.Params)
		log.Printf("added collector: %s, type: %s", name, colCfg.Type)
	}

	// 3. Initialize Sinks
	sinks := make(map[string]sink.Sink)
	for name, sinkCfg := range cfg.Sinks {
		sinkInit, ok := sink.Registry[sinkCfg.Type]
		if !ok {
			log.Printf("unknown sink type: %s", sinkCfg.Type)
			continue
		}
		s, err := sinkInit(sinkCfg.Params)
		if err != nil {
			log.Printf("can't initialize sink %s: %v", name, err)
			continue
		}
		sinks[name] = s
		log.Printf("added sink: %s, type: %s", name, sinkCfg.Type)
	}

	// 5. Start the Pipeline Runner
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := pipeline.NewRunner(eventBus, sinks)
	runner.Start(ctx, cfg.Pipelines)

	// 6. Start Collectors (Injecting the bus so they can publish)
	for _, c := range collectors {
		go c.Start(eventBus)
	}

	// Wait for interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Print("Exiting application...")
	cancel() // Stop pipelines
	for _, c := range collectors {
		c.End() // Stop collectors
	}
}
