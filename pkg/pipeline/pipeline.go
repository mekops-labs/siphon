package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"sync"
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
	state      map[string]interface{} // Pipeline-specific state
	lock       sync.Mutex             // Lock for pipeline-specific state
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

		// Event-driven pipeline
		log.Printf("Pipeline [%s]: event-triggered type active", pCfg.Name)
		go r.runEventPipeline(ctx, cp)
	}
}

// compile prepares the expr programs and templates so they don't re-compile on every event
func (r *Runner) compile(cfg config.PipelineConfig) (*compiledPipeline, error) {
	cp := &compiledPipeline{
		Config:     cfg,
		Transforms: make(map[string]*vm.Program),
		state:      make(map[string]any), // Initialize state during compilation
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

	// Compile Sinks
	var sinkConfigs []config.PipelineSinkConfig
	if len(cfg.Sinks) > 0 {
		sinkConfigs = cfg.Sinks // V2 config places sinks directly under cron
	}

	for _, sCfg := range sinkConfigs {
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
				return nil, fmt.Errorf("failed to compile sink expr '%s': %w", cs.Name, err)
			}
			cs.ExprProg = prog
		case "template":
			fMap := template.FuncMap{
				"now": func(f string) string { return time.Now().Format(f) },
			}

			tmpl, err := template.New(sCfg.Name).Funcs(fMap).Parse(sCfg.Spec)
			if err != nil {
				return nil, fmt.Errorf("failed to compile sink template '%s': %w", cs.Name, err)
			}
			cs.Template = tmpl
		}

		cp.Sinks = append(cp.Sinks, cs)
	}

	return cp, nil
}

// runEventPipeline listens to the bus and triggers processing for matching events
func (r *Runner) runEventPipeline(ctx context.Context, cp *compiledPipeline) {
	for _, topic := range cp.Config.Topics {
		log.Printf("Pipeline [%s]: Subscribing to topic '%s'", cp.Config.Name, topic)

		ch := r.bus.Subscribe(topic)

		// Spawn a listener for each topic this pipeline cares about
		go func(t string, topicChan <-chan bus.Event) {
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-topicChan:
					state := r.updateStateFromEvent(cp, event) // Update state from event
					if state != nil {
						r.dispatchState(cp, state) // Dispatch the specific state snapshot
					}
				}
			}
		}(topic, ch)
	}
}

// runCronPipeline triggers on a schedule and gathers requested state
func (r *Runner) runCronPipeline(ctx context.Context, cp *compiledPipeline) {
	if cp.Config.Stateful {
		// For stateful cron pipelines, we need to continuously consume events
		// and update the pipeline's state. The cron job then dispatches this state.
		for _, topic := range cp.Config.Topics {
			log.Printf("Pipeline [%s]: Subscribing to topic '%s' for state accumulation", cp.Config.Name, topic)
			ch := r.bus.Subscribe(topic)
			go func(topic string, topicChan <-chan bus.Event) {
				for {
					select {
					case <-ctx.Done():
						return
					case event := <-topicChan:
						// Process event to update the pipeline's state
						r.updateStateFromEvent(cp, event) // This handles Ack/Nack
					}
				}
			}(topic, ch)
		}

		// The cron job now only triggers the dispatch of the accumulated state
		s := gocron.NewScheduler(time.UTC)
		s.CronWithSeconds(cp.Config.Schedule).Do(func() {
			r.dispatchAccumulatedState(cp)
		})
		s.StartAsync()
		<-ctx.Done()
		s.Stop()
		return // Exit after setting up state accumulation and cron dispatch
	}

	// Original logic for stateless cron pipelines
	// This part processes events that are available at the time of the cron tick.
	var allChannels []<-chan bus.Event
	for _, topic := range cp.Config.Topics {
		log.Printf("Pipeline [%s]: Subscribing to topic '%s'", cp.Config.Name, topic)

		ch := r.bus.Subscribe(topic)
		allChannels = append(allChannels, ch)
	}

	s := gocron.NewScheduler(time.UTC)
	s.CronWithSeconds(cp.Config.Schedule).Do(func() {
		for _, ch := range allChannels {
			select {
			case event := <-ch: // For stateless cron, process and dispatch each event individually
				state := r.updateStateFromEvent(cp, event)
				if state != nil {
					r.dispatchState(cp, state)
				}
			case <-time.After(10 * time.Millisecond):
				// No event received on this topic during this cron tick, continue to next topic
			}
		}
	})

	s.StartAsync()
	<-ctx.Done()
	s.Stop()
}

// updateStateFromEvent processes an incoming event and updates the pipeline's state.
// It does not perform dispatch.
func (r *Runner) updateStateFromEvent(cp *compiledPipeline, event bus.Event) map[string]interface{} {
	var state map[string]interface{}

	if cp.Config.Stateful {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		state = cp.state
	} else {
		state = make(map[string]interface{})
	}

	// 1. EXTRACT: Parse raw payload into variables
	if cp.Parser != nil {
		extracted, err := cp.Parser.Parse(event.Payload, cp.Config.Parser.Vars)
		if err != nil {
			log.Printf("Parse error in pipeline '%s': %v", cp.Config.Name, err)
			event.Nack() // Nack if parsing fails
			return nil
		}
		// Merge extracted vars into state
		for k, v := range extracted {
			state[k] = v
		}
	} else {
		// If no parser, try to unmarshal raw payload as JSON into a generic "payload" variable
		var genericPayload map[string]interface{}
		if err := json.Unmarshal(event.Payload, &genericPayload); err != nil {
			log.Printf("Failed to unmarshal event payload as JSON in pipeline '%s': %v", cp.Config.Name, err)
			event.Nack() // Nack if we can't parse the payload at all
			return nil
		}
		maps.Copy(state, genericPayload) // Merge the generic payload into state
	}

	// 2. TRANSFORM: Run expr formulas
	for varName, prog := range cp.Transforms {
		result, err := expr.Run(prog, state)
		if err != nil {
			log.Printf("Transform error (%s) in '%s': %v", varName, cp.Config.Name, err)
			// Continue even if transform fails, as other transforms might succeed
			// and we don't want to Nack the event for a transform error.
			continue
		}
		state[varName] = result
	}
	event.Ack() // Acknowledge event after successfully updating state

	if cp.Config.Stateful {
		return maps.Clone(state) // Return a snapshot for safe concurrent dispatch
	}
	return state
}

// dispatchAccumulatedState dispatches the current accumulated state of a pipeline.
// It's typically called by cron jobs or after an event in event-driven pipelines.
func (r *Runner) dispatchAccumulatedState(cp *compiledPipeline) {
	cp.lock.Lock()
	snapshot := maps.Clone(cp.state) // Snapshot under lock
	cp.lock.Unlock()

	r.dispatchState(cp, snapshot)
}

// dispatchState handles the routing of a state snapshot to the bus or configured sinks
func (r *Runner) dispatchState(cp *compiledPipeline, state map[string]interface{}) {
	if len(cp.Sinks) == 0 {
		serializedState, _ := json.Marshal(state)
		r.bus.Publish(cp.Config.Name, serializedState)
		return
	}
	r.dispatchToSinks(cp, state)
}

// dispatchToSinks is the shared formatter logic
func (r *Runner) dispatchToSinks(cp *compiledPipeline, state map[string]interface{}) {
	for _, cs := range cp.Sinks {
		var outputBytes []byte

		if cs.Format == "expr" {
			result, err := expr.Run(cs.ExprProg, state)
			if err != nil {
				log.Printf("Sink expr error [%s]: %v", cs.Name, err)
				continue
			}
			outputBytes, _ = json.Marshal(result)

		} else if cs.Format == "template" {
			var buf bytes.Buffer
			if err := cs.Template.Execute(&buf, state); err != nil {
				log.Printf("Sink template error [%s]: %v", cs.Name, err)
				continue
			}
			outputBytes = buf.Bytes()
		} else {
			// Default to sending the entire state as JSON if no format is specified
			outputBytes, _ = json.Marshal(state)
		}

		if err := cs.Sink.Send(outputBytes); err != nil {
			log.Printf("Sink Send error [%s]: %v", cs.Name, err)
		}
	}
}
