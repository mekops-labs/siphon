package collector

import (
	"github.com/mekops-labs/siphon/pkg/parser"
)

type Collector interface {
	Start() error
	AddDataSource(path string, parser parser.Parser) error
	End()
}

type Init func(params any) Collector
type registry map[string]Init

/* Main registry of all available collector classes */
var Registry = make(registry)

func (c registry) Add(name string, constructor Init) {
	c[name] = constructor
}
