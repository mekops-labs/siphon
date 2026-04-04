package file

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mitchellh/mapstructure"
)

type FileParams struct {
	Interval int `mapstructure:"interval"` // in seconds
}

type fileSource struct {
	params FileParams
	paths  map[string]string
	lock   sync.Mutex
	bus    bus.Bus

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Ensure fileSource implements the Collector interface
var _ collector.Collector = (*fileSource)(nil)

func init() {
	collector.Registry.Add("file", New)
}

func New(p any) collector.Collector {
	var opt FileParams
	if err := mapstructure.Decode(p, &opt); err != nil {
		log.Printf("File collector config error: %v", err)
		return nil
	}

	// Default to 10 seconds if not specified
	if opt.Interval <= 0 {
		opt.Interval = 10
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &fileSource{
		params: opt,
		paths:  make(map[string]string),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (f *fileSource) RegisterTopic(name string, value string) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.paths[name] = value
}

func (f *fileSource) Start(b bus.Bus) {
	f.bus = b
	f.wg.Add(1)

	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(time.Duration(f.params.Interval) * time.Second)
		defer ticker.Stop()

		// Do an immediate initial read
		f.readFiles()

		for {
			select {
			case <-f.ctx.Done():
				return
			case <-ticker.C:
				f.readFiles()
			}
		}
	}()
}

func (f *fileSource) readFiles() {
	f.lock.Lock()
	defer f.lock.Unlock()

	for topic, path := range f.paths {
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("File read error (%s): %v", path, err)
			continue
		}

		// Publish raw file contents to the Event Bus
		if err := f.bus.Publish(topic, data); err != nil {
			log.Printf("File Bus Publish Error (%s): %v", topic, err)
		}
	}
}

func (f *fileSource) End() {
	f.cancel()
	f.wg.Wait()
}
