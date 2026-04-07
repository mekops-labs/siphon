package bus

import (
	"fmt"

	eventBus "github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

type bus struct {
	bus    eventBus.Bus
	params busParams
}

type busParams struct {
	Topic string `mapstructure:"topic"`
}

// Ensure bus implements sink.Sink
var _ sink.Sink = (*bus)(nil)

func init() {
	sink.Registry.Add("bus", New)
}

func New(params any, eventBus eventBus.Bus) (sink.Sink, error) {
	var opt busParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode bus params: %w", err)
	}

	if opt.Topic == "" {
		return nil, fmt.Errorf("bus requires a topic")
	}

	return &bus{bus: eventBus, params: opt}, nil
}

func (b *bus) Send(data []byte) error {
	b.bus.Publish(b.params.Topic, data)
	return nil
}

func (b *bus) Close() error {
	// No resources to clean up for the bus sink
	return nil
}
