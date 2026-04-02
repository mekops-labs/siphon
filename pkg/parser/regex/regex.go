package regex

import (
	"fmt"
	"regexp"

	"github.com/mekops-labs/siphon/pkg/parser"
)

type regexParser struct{}

func init() {
	parser.Register("regex", func() parser.Parser { return &regexParser{} })
}

func (p *regexParser) Parse(payload []byte, vars map[string]string) (map[string]any, error) {
	result := make(map[string]any)
	text := string(payload)

	for varName, regexStr := range vars {
		re, err := regexp.Compile(regexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid regex for %s: %w", varName, err)
		}

		match := re.FindString(text)
		result[varName] = match // In a production app, you might try to parse this to float/int
	}

	return result, nil
}
