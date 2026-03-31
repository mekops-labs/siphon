package collector

import "github.com/mekops-labs/siphon/pkg/bus"

// Collector defines the interface for all ingestion modules
type Collector interface {
	Start(b bus.Bus)
	End()
	RegisterTopic(topic string) // The source_topic from config.yaml
}

type Init func(params any) Collector
type registry map[string]Init

// Main registry of all available collector classes
var Registry = make(registry)

func (c registry) Add(name string, constructor Init) {
	c[name] = constructor
}
