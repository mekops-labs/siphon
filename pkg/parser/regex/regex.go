package parser_regex

import (
	"log"
	"regexp"

	"github.com/antonmedv/expr"
	"github.com/mekops-labs/siphon/pkg/datastore"
	"github.com/mekops-labs/siphon/pkg/parser"
)

type regexParser struct {
	store datastore.DataStore
	vars  map[string]string
	conv  map[string]string
	name  string
}

var _ parser.Parser = (*regexParser)(nil)

func init() {
	parser.Registry.Add("regex", New)
}

func New(name string, store datastore.DataStore) parser.Parser {
	return &regexParser{
		name:  name,
		store: store,
		vars:  make(map[string]string),
		conv:  make(map[string]string),
	}
}

func (j *regexParser) AddVar(name, v string) {
	j.vars[name] = v
}

func (j *regexParser) AddConv(name, v string) {
	j.conv[name] = v
}

func (j *regexParser) Parse(buf []byte) error {

	out := make(map[string]interface{})

	for k, val := range j.vars {
		r, err := regexp.Compile(val)
		if err != nil {
			log.Println(j.name, ": bad regexp: ", err)
			continue
		}

		a := r.FindString(string(buf))
		var value interface{}

		c, ok := j.conv[k]
		if ok {
			value, err = expr.Eval(c, map[string]interface{}{k: a})
			if err != nil {
				log.Printf("%s: bad conversion for variable '%s' (%s): %s", j.name, k, a, err)
				value = a
			}
		} else {
			value = a
		}
		out[k] = value
	}

	j.store.Publish(j.name, out)

	return nil
}
