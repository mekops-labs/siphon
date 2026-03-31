package shell

import (
	"context"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mitchellh/mapstructure"
)

type ShellParams struct {
	Interval int `mapstructure:"interval"` // in seconds
}

type shellSource struct {
	params   ShellParams
	commands []string
	lock     sync.Mutex
	bus      bus.Bus

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Ensure shellSource implements the Collector interface
var _ collector.Collector = (*shellSource)(nil)

func init() {
	collector.Registry.Add("shell", New)
}

func New(p any) collector.Collector {
	var opt ShellParams
	if err := mapstructure.Decode(p, &opt); err != nil {
		log.Printf("Shell collector config error: %v", err)
		return nil
	}

	if opt.Interval <= 0 {
		opt.Interval = 10
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &shellSource{
		params:   opt,
		commands: make([]string, 0),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (s *shellSource) RegisterTopic(topic string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// For the shell collector, the "topic" is the actual shell command
	s.commands = append(s.commands, topic)
	log.Printf("Shell collector registered command: %s", topic)
}

func (s *shellSource) Start(b bus.Bus) {
	s.bus = b
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(time.Duration(s.params.Interval) * time.Second)
		defer ticker.Stop()

		s.executeCommands() // Initial run

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.executeCommands()
			}
		}
	}()
}

func (s *shellSource) executeCommands() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, cmdStr := range s.commands {
		// Use "sh -c" to support piping and shell builtins (like awk/grep)
		cmd := exec.CommandContext(s.ctx, "sh", "-c", cmdStr)

		output, err := cmd.Output() // captures stdout
		if err != nil {
			log.Printf("Shell command failed [%s]: %v", cmdStr, err)
			continue
		}

		// Publish standard output to the Event Bus
		if err := s.bus.Publish(cmdStr, output); err != nil {
			log.Printf("Shell Bus Publish Error [%s]: %v", cmdStr, err)
		}
	}
}

func (s *shellSource) End() {
	s.cancel()
	s.wg.Wait()
}
