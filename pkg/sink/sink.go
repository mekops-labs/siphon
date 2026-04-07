package sink

import "github.com/mekops-labs/siphon/pkg/bus"

type Sink interface {
	Send(b []byte) error
	Close() error
}

type SinkCfg struct {
	Name string
	Type string
	Spec string
}

type Init func(params any, eventBus bus.Bus) (Sink, error)
type registry map[string]Init

var Registry = make(registry)

func (s registry) Add(name string, constructor Init) {
	s[name] = constructor
}
