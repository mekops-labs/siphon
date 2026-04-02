package parser

// Parser defines the interface for converting raw bytes into structured variables.
type Parser interface {
	// Parse takes the raw payload and a map of variable extraction rules (e.g., {"temp": "$.T"})
	// and returns the extracted variables.
	Parse(payload []byte, vars map[string]string) (map[string]any, error)
}

// Registry holds the constructor functions for available parsers
var Registry = make(map[string]func() Parser)

func Register(name string, factory func() Parser) {
	Registry[name] = factory
}
