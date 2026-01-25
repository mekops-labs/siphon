package cron

import (
	"bytes"
	"encoding/json"
	"log"
	"text/template"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/mekops-labs/siphon/pkg/datastore"
	"github.com/mekops-labs/siphon/pkg/dispatcher"
	"github.com/mekops-labs/siphon/pkg/sink"

	"github.com/antonmedv/expr"
)

type sinkInstance struct {
	iface sink.Sink
	cfg   sink.SinkCfg
}

type cronDispatcher struct {
	cronString string
	sinks      []sinkInstance
	ds         datastore.DataStore
}

var _ dispatcher.Dispatcher = (*cronDispatcher)(nil)

func init() {
	dispatcher.Registry.Add("cron", New)
}

func New(param any, ds datastore.DataStore) dispatcher.Dispatcher {
	cronString := param.(string)
	return &cronDispatcher{
		cronString: cronString,
		ds:         ds,
		sinks:      make([]sinkInstance, 0),
	}
}

func (c *cronDispatcher) sendToAll() {
	for _, s := range c.sinks {

		var toSend []byte

		switch s.cfg.Type {
		case "expr":
			output, err := expr.Eval(s.cfg.Spec, c.ds.Map())
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
			t := template.Must(template.New("msg").Parse(s.cfg.Spec))
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

func (c *cronDispatcher) Start() {
	s := gocron.NewScheduler(time.UTC)
	s.CronWithSeconds(c.cronString).Do(c.sendToAll)

	s.StartAsync()
}

func (c *cronDispatcher) AddSink(s sink.Sink, cfg sink.SinkCfg) {
	sink := sinkInstance{
		iface: s,
		cfg:   cfg,
	}
	c.sinks = append(c.sinks, sink)
}
