package main

import (
	"context"
	"flag"
	"fmt"
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
	editorPort := flag.Int("editor-port", 0, "Enable the web-based config editor on this port (e.g. 8099)")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("need config file path as argument! Usage: siphon [flags] <config.yaml>")
	}
	configPath := flag.Arg(0)

	log.Print("Starting Siphon v. ", version)

	reload := make(chan struct{}, 1)
	status := editor.NewStatus()

	// The editor starts unconditionally and runs for the entire process lifetime.
	// This ensures it stays reachable even when the engine fails due to config errors.
	if *editorPort > 0 {
		go editor.Start(*editorPort, configPath, reload, status)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	cancel, stop := startEngine(configPath, status)

	for {
		select {
		case sig := <-sigs:
			cancel()
			stop()
			if sig == syscall.SIGHUP {
				log.Print("SIGHUP received — reloading config...")
				cancel, stop = startEngine(configPath, status)
			} else {
				log.Print("Exiting application...")
				return
			}

		case <-reload:
			log.Print("Config saved via editor — reloading engine...")
			cancel()
			stop()
			cancel, stop = startEngine(configPath, status)
		}
	}
}

// startEngine loads config and initializes all components. On config error it
// records the failure in status and returns no-op functions, leaving the editor
// unaffected and reachable for the user to fix the config.
func startEngine(configPath string, status *editor.Status) (context.CancelFunc, func()) {
	noop := func() {}

	cfg, err := config.Load(configPath)
	if err != nil {
		msg := fmt.Sprintf("config error: %v", err)
		log.Printf("Engine not started: %s", msg)
		status.Set(false, msg)
		return noop, noop
	}

	eventBus := bus.NewMemoryBus()

	collectors := make(map[string]collector.Collector)
	for name, colCfg := range cfg.Collectors {
		collectorInit, ok := collector.Registry[colCfg.Type]
		if !ok {
			log.Printf("unknown collector type: %s", colCfg.Type)
			continue
		}
		c := collectorInit(colCfg.Params)
		for alias, rawTopic := range colCfg.Topics {
			c.RegisterTopic(alias, rawTopic)
			log.Printf("Collector [%s] registered alias '%s' for '%s'", name, alias, rawTopic)
		}
		collectors[name] = c
	}

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

	ctx, cancel := context.WithCancel(context.Background())

	runner := pipeline.NewRunner(eventBus, sinks)
	runner.Start(ctx, cfg.Pipelines)

	for _, c := range collectors {
		go c.Start(eventBus)
	}

	status.Set(true, "Running")
	log.Print("Engine started successfully")

	return cancel, func() {
		runner.Close()
		for _, c := range collectors {
			c.End()
		}
	}
}
