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
		go editor.Start(*editorPort, configPath, reload, status, version)
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

// buildCollectors initializes all collectors from the config. Unknown types are
// skipped and a warning is appended; all other collectors are registered and returned.
func buildCollectors(cfg *config.Config) (map[string]collector.Collector, []string) {
	collectors := make(map[string]collector.Collector)
	var warnings []string
	for name, colCfg := range cfg.Collectors {
		collectorInit, ok := collector.Registry[colCfg.Type]
		if !ok {
			w := fmt.Sprintf("collector %q: unknown type %q", name, colCfg.Type)
			log.Print(w)
			warnings = append(warnings, w)
			continue
		}
		c := collectorInit(colCfg.Params)
		for alias, rawTopic := range colCfg.Topics {
			c.RegisterTopic(alias, rawTopic)
			log.Printf("Collector [%s] registered alias '%s' for '%s'", name, alias, rawTopic)
		}
		collectors[name] = c
	}
	return collectors, warnings
}

// buildSinks initializes all sinks from the config. Unknown types and init errors
// are skipped; a warning is appended for each failure.
func buildSinks(cfg *config.Config, eventBus bus.Bus) (map[string]sink.Sink, []string) {
	sinks := make(map[string]sink.Sink)
	var warnings []string
	for name, sinkCfg := range cfg.Sinks {
		sinkInit, ok := sink.Registry[sinkCfg.Type]
		if !ok {
			w := fmt.Sprintf("sink %q: unknown type %q", name, sinkCfg.Type)
			log.Print(w)
			warnings = append(warnings, w)
			continue
		}
		s, err := sinkInit(sinkCfg.Params, eventBus)
		if err != nil {
			w := fmt.Sprintf("sink %q: %v", name, err)
			log.Printf("can't initialize sink %s: %v", name, err)
			warnings = append(warnings, w)
			continue
		}
		sinks[name] = s
		log.Printf("added sink: %s, type: %s", name, sinkCfg.Type)
	}
	return sinks, warnings
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

	collectors, collectorWarnings := buildCollectors(cfg)
	sinks, sinkWarnings := buildSinks(cfg, eventBus)

	var warnings []string
	warnings = append(warnings, collectorWarnings...)
	warnings = append(warnings, sinkWarnings...)

	ctx, cancel := context.WithCancel(context.Background())

	runner := pipeline.NewRunner(eventBus, sinks)
	pipelineCount, pipelineWarnings := runner.Start(ctx, cfg.Pipelines)
	warnings = append(warnings, pipelineWarnings...)

	for _, c := range collectors {
		go c.Start(eventBus)
	}

	status.SetRunning(pipelineCount, len(collectors), len(sinks), warnings)
	log.Print("Engine started successfully")

	return cancel, func() {
		runner.Close()
		for _, c := range collectors {
			c.End()
		}
	}
}
