package jsonpath

import (
	"encoding/json"
	"fmt"

	"github.com/mekops-labs/siphon/pkg/parser"

	"github.com/PaesslerAG/jsonpath"
)

type jsonpathParser struct{}

func init() {
	parser.Register("jsonpath", func() parser.Parser { return &jsonpathParser{} })
}

func (_ *jsonpathParser) Parse(payload []byte, vars map[string]string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	v := interface{}(nil)

	err := json.Unmarshal(payload, &v)
	if err != nil {
		return nil, fmt.Errorf("invalid json in payload: %w", err)
	}

	for varName, jsonPath := range vars {
		value, err := jsonpath.Get(jsonPath, v)
		if err != nil {
			return nil, fmt.Errorf("jsonpath error for %s: %w", varName, err)
		}

		result[varName] = value
	}

	return result, nil
}
