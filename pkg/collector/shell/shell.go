package collector_shell

import (
	"log"
	"os/exec"
	"time"

	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mekops-labs/siphon/pkg/parser"
	"github.com/mitchellh/mapstructure"
)

type source struct {
	command string
	parser  parser.Parser
}

type shellCollector struct {
	sources  []source
	interval int
	end      chan bool
}

/* Main config structure. Descibes `params` field from configuration yaml. */
type ShellParams struct {
	Interval int
}

var _ collector.Collector = (*shellCollector)(nil)

/* In this module init function we register our collector in global collector registry */
func init() {
	collector.Registry.Add("shell", New)
}

func New(p any) collector.Collector {
	var opt ShellParams

	if err := mapstructure.Decode(p, &opt); err != nil {
		return nil
	}

	// Set defaults
	if opt.Interval == 0 {
		opt.Interval = 60
	}

	return &shellCollector{
		sources:  make([]source, 0),
		interval: opt.Interval,
		end:      make(chan bool),
	}
}

func (f *shellCollector) runAndParse() {
	for n, i := range f.sources {
		cmd := exec.Command("sh", "-c", i.command)
		buf, err := cmd.Output()
		if err != nil {
			log.Print(n, ": can't execute - ", err)
			continue
		}
		i.parser.Parse(buf)
	}
}

func (f *shellCollector) Start() error {
	go func() {
		for len(f.sources) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		f.runAndParse()
	loop:
		for {
			select {
			case <-f.end:
				f.end <- false
				close(f.end)
				break loop
			case <-time.After(time.Duration(f.interval) * time.Second):
				f.runAndParse()
			}
		}
	}()
	return nil
}

func (f *shellCollector) AddDataSource(path string, parser parser.Parser) error {
	f.sources = append(f.sources, source{
		command: path,
		parser:  parser,
	})
	return nil
}

func (f *shellCollector) End() {
	f.end <- true
	<-f.end
}
