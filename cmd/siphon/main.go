package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mekops-labs/siphon/internal/config"
	_ "github.com/mekops-labs/siphon/internal/modules"

	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mekops-labs/siphon/pkg/sink"
)

var version = "unknown"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("need config file!")
	}

	log.Print("Starting Siphon v", version)

	// 1. Load configuration
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

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

	// 4. Initialize Pipelines (Replaces the old Data and Dispatchers blocks)
	for _, pipelineCfg := range cfg.Pipelines {
		// TODO: Implementation for mapping pipeline nodes to the HybridBus goes here.
		log.Printf("added pipeline: %s", pipelineCfg.Name)
	}

	// Start Collectors
	for _, c := range collectors {
		c.Start()
	}

	// Wait for interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Print("Exiting application...")
	for _, c := range collectors {
		c.End()
	}
}
