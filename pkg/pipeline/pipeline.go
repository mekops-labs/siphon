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

type transformProgram struct {
	VarName string
	Program *vm.Program
}

// compiledPipeline holds the runtime state of a pipeline
type compiledPipeline struct {
	Config     config.PipelineConfig
	Parser     parser.Parser
	Transforms []transformProgram
	Sinks      []compiledSink
	state      map[string]interface{}    // Pipeline-specific state
	topicData  map[string]map[string]any // Stores latest parsed data for each topic, keyed by topic name
	stateLock  sync.Mutex                // Lock for pipeline-specific state updates
	dispatchMu sync.Mutex                // Ensures ordered dispatch for stateful pipelines
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

// Close gracefully shuts down all sinks managed by the runner
func (r *Runner) Close() {
	for name, s := range r.sinks {
		if err := s.Close(); err != nil {
			log.Printf("Failed to close sink '%s': %v", name, err)
		}
	}
}

// Start initializes all configured pipelines. It returns the number of
// pipelines that started successfully and a slice of warning strings for
// any pipelines that failed to compile.
func (r *Runner) Start(ctx context.Context, pipelines []config.PipelineConfig) (started int, warnings []string) {
	for _, pCfg := range pipelines {
		cp, err := r.compile(pCfg)
		if err != nil {
			log.Printf("Failed to compile pipeline '%s': %v", pCfg.Name, err)
			warnings = append(warnings, fmt.Sprintf("pipeline %q: %v", pCfg.Name, err))
			continue
		}

		if pCfg.Type == "cron" {
			log.Printf("Pipeline [%s]: Cron active with schedule '%s'", pCfg.Name, pCfg.Schedule)
			go r.runCronPipeline(ctx, cp)
		} else {
			log.Printf("Pipeline [%s]: event-triggered type active", pCfg.Name)
			go r.runEventPipeline(ctx, cp)
		}
		started++
	}
	return started, warnings
}

// compile prepares the expr programs and templates so they don't re-compile on every event
func (r *Runner) compile(cfg config.PipelineConfig) (*compiledPipeline, error) {
	cp := &compiledPipeline{
		Config:     cfg,
		Transforms: make([]transformProgram, 0, len(cfg.Transform)), // Initialize as slice
		state:      make(map[string]any),                            // Initialize state during compilation
		topicData:  make(map[string]map[string]any),                 // Initialize new map
	}

	// Setup Parser
	if cfg.Parser != nil {
		factory, ok := parser.Registry[cfg.Parser.Type]
		if !ok {
			return nil, fmt.Errorf("unknown parser type: %s", cfg.Parser.Type)
		}
		cp.Parser = factory()
	}

	// Helper function for compiling expr programs
	compileExprProgram := func(exprStr string) (*vm.Program, error) {
		exprOpts := []expr.Option{
			expr.Env(map[string]any{}), // The environment will be passed at runtime
			expr.AllowUndefinedVariables(),
		}
		return expr.Compile(exprStr, exprOpts...)
	}

	for _, transformation := range cfg.Transform {
		var varName, exprStr string
		for varName, exprStr = range transformation {
			if varName == "" || exprStr == "" {
				return nil, fmt.Errorf("invalid transform entry: variable name and expression must be non-empty")
			}
			break // We only expect one key-value pair per transform map
		}
		prog, err := compileExprProgram(exprStr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile transform '%s': %w", varName, err)
		}
		cp.Transforms = append(cp.Transforms, transformProgram{VarName: varName, Program: prog})
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
				case event := <-topicChan: // Pass the topic 't'
					state := r.updateStateFromEvent(cp, t, event) // Update state from event
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
						// Process event to update the pipeline's state, no immediate dispatch
						r.updateStateFromEvent(cp, topic, event) // This handles Ack/Nack
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
	// This part processes events that are available on the bus at the time of the cron tick.
	var topicChannels []struct {
		topic string
		ch    <-chan bus.Event
	}
	for _, topic := range cp.Config.Topics {
		log.Printf("Pipeline [%s]: Subscribing to topic '%s'", cp.Config.Name, topic)
		ch := r.bus.Subscribe(topic)
		topicChannels = append(topicChannels, struct {
			topic string
			ch    <-chan bus.Event
		}{topic, ch})
	}

	s := gocron.NewScheduler(time.UTC)
	s.CronWithSeconds(cp.Config.Schedule).Do(func() {
		for _, tc := range topicChannels { // Iterate over topic-channel pairs
			select {
			case event := <-tc.ch: // For stateless cron, process and dispatch each event individually
				state := r.updateStateFromEvent(cp, tc.topic, event) // Pass tc.topic
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
// It returns a snapshot of the state for immediate dispatch if stateless or a stateful event pipeline,
// or nil if it's a stateful cron pipeline (as dispatch happens on cron tick).
func (r *Runner) updateStateFromEvent(cp *compiledPipeline, eventTopic string, event bus.Event) map[string]any {
	var parsedEventData map[string]any

	// 1. EXTRACT: Parse raw payload into variables
	if cp.Parser != nil {
		extracted, err := cp.Parser.Parse(event.Payload, cp.Config.Parser.Vars)
		if err != nil {
			log.Printf("Parse error in pipeline '%s' for topic '%s': %v", cp.Config.Name, eventTopic, err)
			event.Nack() // Nack if parsing fails
			return nil
		}
		parsedEventData = extracted
	} else {
		// If no parser, try to unmarshal raw payload as JSON into a generic "payload" variable
		var genericPayload map[string]any
		if err := json.Unmarshal(event.Payload, &genericPayload); err != nil {
			log.Printf("Failed to unmarshal event payload as JSON in pipeline '%s' for topic '%s': %v", cp.Config.Name, eventTopic, err)
			event.Nack() // Nack if we can't parse the payload at all
			return nil
		}
		if genericPayload == nil {
			genericPayload = make(map[string]any)
		}
		parsedEventData = genericPayload
	}

	if parsedEventData == nil {
		parsedEventData = make(map[string]any)
	}

	cp.stateLock.Lock()
	defer cp.stateLock.Unlock()

	// Prepare environment for transforms
	var transformEnv map[string]any
	if cp.Config.Stateful {
		// Update topic-specific data store so it's available via topicName.variableName in transforms
		cp.topicData[eventTopic] = parsedEventData
		transformEnv = maps.Clone(cp.state) // Start with pipeline-level accumulated state
		// Inject all known topic data into the transform environment
		for t, data := range cp.topicData {
			transformEnv[t] = data
		}
	} else {
		transformEnv = make(map[string]any)
		// For stateless, only the current topic's data is available nested
		transformEnv[eventTopic] = parsedEventData
	}
	maps.Copy(transformEnv, parsedEventData) // Merge current event's parsed data into transformEnv

	// 2. TRANSFORM: Run expr formulas
	for _, ot := range cp.Transforms { // Iterate over ordered slice
		result, err := expr.Run(ot.Program, transformEnv)
		if err != nil {
			log.Printf("Transform error (%s) in '%s' for topic '%s': %v", ot.VarName, cp.Config.Name, eventTopic, err)
			// Continue even if transform fails, as other transforms might succeed
			// and we don't want to Nack the event for a transform error.
			continue
		}
		transformEnv[ot.VarName] = result
	}
	event.Ack() // Acknowledge event after successfully updating state

	if cp.Config.Stateful {
		// Update pipeline's accumulated state (cp.state) with results from transforms
		// Only update variables that are explicitly defined in transforms.
		for _, ot := range cp.Transforms {
			if val, ok := transformEnv[ot.VarName]; ok {
				cp.state[ot.VarName] = val
			}
		}
	}

	// Decide what to return for dispatch
	if cp.Config.Type == "cron" && cp.Config.Stateful {
		// Stateful cron pipelines dispatch on cron tick, not immediately after event
		return nil
	}

	var dispatchEnv map[string]any

	if !cp.Config.Stateful {
		// Stateless (event or cron): return parsed event data at root, transform results, and topic data.
		dispatchEnv = make(map[string]any)
		// 1. Add parsed event data at the root for direct access by current pipeline's sinks/expr
		maps.Copy(dispatchEnv, parsedEventData)

		// 2. Add transform results, potentially overwriting parsed data if var names clash
		for _, ot := range cp.Transforms { // This loop is skipped if no transforms
			if val, ok := transformEnv[ot.VarName]; ok {
				dispatchEnv[ot.VarName] = val
			}
		}
		// 3. Add the full parsed event data nested under its topic name for downstream pipelines
		dispatchEnv[eventTopic] = parsedEventData
		return dispatchEnv
	} else {
		// Stateful event (or cron fallback): return full accumulated state
		dispatchEnv = maps.Clone(cp.state) // Start with pipeline-level accumulated state
		for topic, data := range cp.topicData {
			dispatchEnv[topic] = data
		}
	}
	return dispatchEnv
}

// dispatchAccumulatedState dispatches the current accumulated state of a pipeline.
// It's typically called by cron jobs or after an event in event-driven pipelines.
func (r *Runner) dispatchAccumulatedState(cp *compiledPipeline) {
	cp.stateLock.Lock()
	dispatchEnv := maps.Clone(cp.state) // Start with pipeline-level accumulated state
	// Add topic-specific data as nested maps
	for topic, data := range cp.topicData {
		dispatchEnv[topic] = data
	}
	cp.stateLock.Unlock()

	r.dispatchState(cp, dispatchEnv)
}

// dispatchState handles the routing of a state snapshot to the bus or configured sinks
func (r *Runner) dispatchState(cp *compiledPipeline, state map[string]any) {
	if state == nil {
		return
	}

	if cp.Config.Stateful {
		cp.dispatchMu.Lock()
		defer cp.dispatchMu.Unlock()
	}

	if len(cp.Sinks) == 0 {
		serializedState, _ := json.Marshal(state)
		r.bus.Publish(cp.Config.Name, serializedState)
		return
	}
	r.dispatchToSinks(cp, state)
}

// dispatchToSinks is the shared formatter logic
func (r *Runner) dispatchToSinks(cp *compiledPipeline, state map[string]any) {
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
