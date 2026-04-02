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

		match := re.FindStringSubmatch(text)
		if match == nil {
			result[varName] = "" // No match found, return empty string
			continue
		}

		if len(match) > 1 {
			result[varName] = match[1] // Use the first capturing group
		} else {
			result[varName] = match[0] // No capturing group, use the whole match
		}
	}

	return result, nil
}
