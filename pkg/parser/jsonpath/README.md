# JSONPath Parser

The `jsonpath` parser extracts specific values from a JSON payload and assigns them to variables for use in the pipeline
transformation phase.

## Configuration

Add the parser to your pipeline and define the extraction rules in the `vars` section. Each key is the variable name,
and the value is the JSONPath expression.

```yaml
pipelines:
  - name: "weather-processor"
    source_topic: "telemetry/weather"
    parser:
      type: "jsonpath"
      vars:
        temp: "$.main.temp"
        humidity: "$.main.humidity"
        city: "$.name"
```

## Syntax Guide

This implementation follows standard JSONPath syntax:

- `$` : The root object or element.
- `$.field` : Selects a child member by name.
- `$.nested.field` : Selects a deeply nested child member.
- `$.items[0]` : Selects the first element of an array.
