package dispatcher

import (
	"github.com/mekops-labs/siphon/pkg/datastore"
	"github.com/mekops-labs/siphon/pkg/sink"
)

type Dispatcher interface {
	Start()
	AddSink(sink sink.Sink, cfg sink.SinkCfg)
}

type Init func(param any, ds datastore.DataStore) Dispatcher
type registry map[string]Init

/* Main registry of all available collector classes */
var Registry = make(registry)

func (r registry) Add(name string, constructor Init) {
	r[name] = constructor
}
