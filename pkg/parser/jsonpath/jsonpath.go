package jsonpath

import (
	"encoding/json"
	"log"

	"github.com/mekops-labs/siphon/pkg/datastore"
	"github.com/mekops-labs/siphon/pkg/parser"

	jp "github.com/PaesslerAG/jsonpath"
	"github.com/antonmedv/expr"
)

type jsonpathParser struct {
	store datastore.DataStore
	vars  map[string]string
	conv  map[string]string
	name  string
}

var _ parser.Parser = (*jsonpathParser)(nil)

func init() {
	parser.Registry.Add("jsonpath", New)
}

func New(name string, store datastore.DataStore) parser.Parser {
	return &jsonpathParser{
		name:  name,
		store: store,
		vars:  make(map[string]string),
		conv:  make(map[string]string),
	}
}

func (j *jsonpathParser) AddVar(name, v string) {
	j.vars[name] = v
}

func (j *jsonpathParser) AddConv(name, v string) {
	j.conv[name] = v
}

func (j *jsonpathParser) Parse(buf []byte) error {
	v := interface{}(nil)

	err := json.Unmarshal(buf, &v)
	if err != nil {
		log.Println("bad json: ", err)
		return err
	}

	out := make(map[string]interface{})

	for k, val := range j.vars {
		a, err := jp.Get(val, v)
		if err != nil {
			log.Println("bad jsonpath: ", err)
			goto fail
		}
		var value interface{}

		c, ok := j.conv[k]
		if ok {
			value, err = expr.Eval(c, map[string]interface{}{k: a})
			if err != nil {
				log.Println(j.name, ": bad conversion for variable", k, ":", err)
				value = a
			}
		} else {
			value = a
		}
		out[k] = value
	}

	j.store.Publish(j.name, out)

fail:
	return err
}
