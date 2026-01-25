package event

import (
	"bytes"
	"encoding/json"
	"log"
	"text/template"
	"time"

	"github.com/mekops-labs/siphon/pkg/datastore"
	"github.com/mekops-labs/siphon/pkg/dispatcher"
	"github.com/mekops-labs/siphon/pkg/sink"

	"github.com/antonmedv/expr"
	"github.com/mitchellh/mapstructure"
)

type sinkInstance struct {
	iface sink.Sink
	cfg   sink.SinkCfg
}

type EventParams struct {
	Trigger string
	Var     string
	Expr    string
	Timeout uint
}

type eventDispatcher struct {
	eventParams EventParams
	sinks       []sinkInstance
	ds          datastore.DataStore
	triggered   bool
}

type triggerExpr struct {
	New any `expr:"new"`
	Old any `expr:"old"`
}

var _ dispatcher.Dispatcher = (*eventDispatcher)(nil)

func init() {
	dispatcher.Registry.Add("event", New)
}

func New(param any, ds datastore.DataStore) dispatcher.Dispatcher {
	var opt EventParams

	if err := mapstructure.Decode(param, &opt); err != nil {
		return nil
	}

	c := &eventDispatcher{
		eventParams: opt,
		ds:          ds,
		sinks:       make([]sinkInstance, 0),
	}

	c.ds.Register([]string{c.eventParams.Trigger}, c.sendToAll, time.Duration(opt.Timeout)*time.Second, c.alert)

	return c
}

func (c *eventDispatcher) dispatch(timeout bool) {
	for _, s := range c.sinks {

		var toSend []byte

		switch s.cfg.Type {
		case "expr":
			env := c.ds.Map()
			env["IsTimeout"] = func() bool { return timeout }
			output, err := expr.Eval(s.cfg.Spec, env)
			if err != nil {
				log.Println("can't run expr: ", err)
				continue
			}

			toSend, err = json.Marshal(output)
			if err != nil {
				log.Println("can't encode json: ", err)
				continue
			}
		case "template":
			fMap := template.FuncMap{"IsTimeout": func() bool { return timeout }}
			t := template.Must(template.New("msg").Funcs(fMap).Parse(s.cfg.Spec))
			b := &bytes.Buffer{}
			err := t.Execute(b, c.ds.Map())
			if err != nil {
				log.Print("can't execute template: ", err)
				continue
			}
			toSend = b.Bytes()
		default:
			log.Printf("unknown sink (%s) data type: %s", s.cfg.Name, s.cfg.Type)
			continue
		}

		log.Print("Sending to: ", s.cfg.Name)
		err := s.iface.Send(toSend)
		if err != nil {
			log.Print("Sink error: ", err)
		}
	}
}

func (c *eventDispatcher) sendToAll(key string, t time.Time, v, old any) {

	var commence bool

	_, ok := v.(map[string]any)
	if !ok {
		log.Print("input error")
		return
	}
	newV, ok := v.(map[string]any)[c.eventParams.Var]
	if !ok {
		log.Print("var not found: ", c.eventParams.Var)
		return
	}

	_, ok = old.(map[string]any)
	if !ok {
		return
	}

	oldV, ok := old.(map[string]any)[c.eventParams.Var]
	if !ok {
		log.Print("var not found: ", c.eventParams.Var)
		return
	}

	values := triggerExpr{newV, oldV}

	o, err := expr.Eval(c.eventParams.Expr, values)
	if err != nil {
		log.Print("expr error: ", err)
		return
	}

	commence, ok = o.(bool)
	if !ok {
		log.Print("expr output value must be boolean")
		return
	}

	if !commence {
		c.triggered = false
		return
	}

	if !c.triggered {
		c.triggered = true
		c.dispatch(false)
	}
}

func (c *eventDispatcher) alert(key string, t time.Time, v any) {
	c.dispatch(true)
}

func (c *eventDispatcher) Start() {

}

func (c *eventDispatcher) AddSink(s sink.Sink, cfg sink.SinkCfg) {
	sink := sinkInstance{
		iface: s,
		cfg:   cfg,
	}
	c.sinks = append(c.sinks, sink)
}
