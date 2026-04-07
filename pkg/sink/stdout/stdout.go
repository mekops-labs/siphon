package stdout

import (
	"fmt"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
)

type stdout struct{}

var _ sink.Sink = (*stdout)(nil)

func init() {
	sink.Registry.Add("stdout", New)
}

func New(_ any, _ bus.Bus) (sink.Sink, error) {
	return &stdout{}, nil
}

func (*stdout) Send(b []byte) error {
	fmt.Println(string(b))
	return nil
}

func (*stdout) Close() error {
	return nil
}
