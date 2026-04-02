package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"text/template"
	"time"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/go-co-op/gocron"
	"github.com/mekops-labs/siphon/internal/config"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/parser"
	"github.com/mekops-labs/siphon/pkg/sink"
)

// compiledSink holds the pre-compiled formatters for maximum performance
type compiledSink struct {
	Name     string
	Sink     sink.Sink
	Format   string
	ExprProg *vm.Program
	Template *template.Template
}

// compiledPipeline holds the runtime state of a pipeline
type compiledPipeline struct {
	Config     config.PipelineConfig
	Parser     parser.Parser
	Transforms map[string]*vm.Program
	Sinks      []compiledSink
}

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
		cp, err := r.compile(pCfg)
		if err != nil {
			log.Printf("Failed to compile pipeline '%s': %v", pCfg.Name, err)
			continue
		}

		if pCfg.Type == "cron" {
			log.Printf("Pipeline [%s]: Cron active with schedule '%s'", pCfg.Name, pCfg.Schedule)
			go r.runCronPipeline(ctx, cp)
			continue
		}

		log.Printf("Pipeline [%s]: Active and listening on '%s'", pCfg.Name, pCfg.SourceTopic)
		go r.runEventPipeline(ctx, cp)
	}
}

// compile prepares the expr programs and templates so they don't re-compile on every event
func (r *Runner) compile(cfg config.PipelineConfig) (*compiledPipeline, error) {
	cp := &compiledPipeline{
		Config:     cfg,
		Transforms: make(map[string]*vm.Program),
	}

	// Setup Parser
	if cfg.Parser != nil {
		factory, ok := parser.Registry[cfg.Parser.Type]
		if !ok {
			return nil, fmt.Errorf("unknown parser type: %s", cfg.Parser.Type)
		}
		cp.Parser = factory()
	}

	// Compile Transformations (expr)
	for varName, exprStr := range cfg.Transform {
		// We use expr.Env to allow variables in the expression
		exprOpts := []expr.Option{
			expr.Env(map[string]any{}),
			expr.AllowUndefinedVariables(),
		}

		prog, err := expr.Compile(exprStr, exprOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to compile transform '%s': %w", varName, err)
		}
		cp.Transforms[varName] = prog
	}

	// Compile Sinks and Formatters
	if cfg.Dispatch != nil {
		for _, sCfg := range cfg.Dispatch.Sinks {
			targetSink, ok := r.sinks[sCfg.Name]
			if !ok {
				return nil, fmt.Errorf("sink not found: %s", sCfg.Name)
			}

			cs := compiledSink{Name: sCfg.Name, Sink: targetSink, Format: sCfg.Format}

			switch sCfg.Format {
			case "expr":
				exprOpts := []expr.Option{
					expr.Env(map[string]any{}),
					expr.AllowUndefinedVariables(),
				}

				prog, err := expr.Compile(sCfg.Spec, exprOpts...)
				if err != nil {
					return nil, fmt.Errorf("failed to compile sink expr: %w", err)
				}
				cs.ExprProg = prog
			case "template":
				tmpl, err := template.New(sCfg.Name).Parse(sCfg.Spec)
				if err != nil {
					return nil, fmt.Errorf("failed to compile sink template: %w", err)
				}
				cs.Template = tmpl
			}

			cp.Sinks = append(cp.Sinks, cs)
		}
	}
	return cp, nil
}

func (r *Runner) runEventPipeline(ctx context.Context, cp *compiledPipeline) {
	ch := r.bus.Subscribe(cp.Config.SourceTopic)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			r.processEvent(cp, event)
		}
	}
}

func (r *Runner) runCronPipeline(ctx context.Context, cp *compiledPipeline) {
	ch := r.bus.Subscribe(cp.Config.SourceTopic)
	s := gocron.NewScheduler(time.UTC)
	s.CronWithSeconds(cp.Config.Schedule).Do(func() {
		r.processEvent(cp, <-ch)
	})

	s.StartAsync()
	<-ctx.Done()
	s.Stop()
}

// processEvent executes the core (E)xtract(T)ransform(L)oad logic
func (r *Runner) processEvent(cp *compiledPipeline, event bus.Event) {
	state := make(map[string]interface{})

	// 1. EXTRACT: Parse raw payload into variables
	if cp.Parser != nil {
		extracted, err := cp.Parser.Parse(event.Payload, cp.Config.Parser.Vars)
		if err != nil {
			log.Printf("Parse error in pipeline '%s': %v", cp.Config.Name, err)
			event.Nack()
			return
		}
		// Merge extracted vars into state
		maps.Copy(state, extracted)
	}

	// 2. TRANSFORM: Run expr formulas
	for varName, prog := range cp.Transforms {
		result, err := expr.Run(prog, state)
		if err != nil {
			log.Printf("Transform error (%s) in '%s': %v", varName, cp.Config.Name, err)
			continue
		}
		state[varName] = result
	}

	// 3. LOAD: Format and Dispatch to Sinks
	for _, cs := range cp.Sinks {
		var outputBytes []byte

		if cs.Format == "expr" {
			// Run the expr to generate a data structure, then JSON marshal it
			result, err := expr.Run(cs.ExprProg, state)
			if err != nil {
				log.Printf("Sink format expr error: %v", err)
				continue
			}
			outputBytes, _ = json.Marshal(result)

		} else if cs.Format == "template" {
			// Run standard Go text/template
			var buf bytes.Buffer
			if err := cs.Template.Execute(&buf, state); err != nil {
				log.Printf("Sink template error: %v", err)
				continue
			}
			outputBytes = buf.Bytes()
		} else {
			// Fallback: send raw payload
			outputBytes = event.Payload
		}

		if err := cs.Sink.Send(outputBytes); err != nil {
			log.Printf("Sink Send error [%s]: %v", cs.Name, err)
		}
	}

	event.Ack()
}
