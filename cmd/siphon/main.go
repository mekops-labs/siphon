package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mekops-labs/siphon/internal/config"
	_ "github.com/mekops-labs/siphon/internal/modules"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mekops-labs/siphon/pkg/editor"
	"github.com/mekops-labs/siphon/pkg/pipeline"
	"github.com/mekops-labs/siphon/pkg/sink"
)

var version = "unknown"

func main() {
	// Add an optional flag for the editor port
	editorPort := flag.Int("editor-port", 0, "Enable the web-based config editor on this port (e.g. 8099)")
	flag.Parse()

	// Read the config file path from the remaining args
	if flag.NArg() < 1 {
		log.Fatal("need config file path as argument! Usage: siphon [flags] <config.yaml>")
	}
	configPath := flag.Arg(0)

	log.Print("Starting Siphon v. ", version)

	if *editorPort > 0 {
		go editor.Start(*editorPort, configPath)
	}

	// 1. Load Config
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal(err)
	}

	// 2. Initialize the Event Bus
	eventBus := bus.NewMemoryBus()

	// 3. Initialize Collectors
	collectors := make(map[string]collector.Collector)
	for name, colCfg := range cfg.Collectors {
		collectorInit, ok := collector.Registry[colCfg.Type]
		if !ok {
			log.Printf("unknown collector type: %s", colCfg.Type)
			continue
		}

		c := collectorInit(colCfg.Params)

		// Register all aliases defined in config.yaml for this collector
		for alias, rawTopic := range colCfg.Topics {
			c.RegisterTopic(alias, rawTopic)
			log.Printf("Collector [%s] registered alias '%s' for '%s'", name, alias, rawTopic)
		}

		collectors[name] = c
	}

	// 4. Initialize Sinks
	sinks := make(map[string]sink.Sink)
	for name, sinkCfg := range cfg.Sinks {
		sinkInit, ok := sink.Registry[sinkCfg.Type]
		if !ok {
			log.Printf("unknown sink type: %s", sinkCfg.Type)
			continue
		}
		s, err := sinkInit(sinkCfg.Params, eventBus)
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

	// 7. Wait for interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Print("Exiting application...")
	cancel() // Stop pipelines
	for _, c := range collectors {
		c.End() // Stop collectors
	}
}
