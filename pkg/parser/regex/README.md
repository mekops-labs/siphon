# Regex Parser

The `regex` parser extracts values from raw text payloads using Regular Expressions.

## Configuration

Define the variables you want to extract by providing a valid Go regular expression for each.

```yaml
pipelines:
  - name: "log-monitor"
    source_topic: "system/logs"
    parser:
      type: "regex"
      vars:
        level: "INFO|WARN|ERROR"
        ip: '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        value: 'temp: ([0-9.]+)'
```

## Important Notes

1. **Match Result**: This parser currently returns the **entire text** of the first match found in the payload for each
   expression.
2. **No Match**: If a regular expression does not find a match, the resulting variable will be an empty string.
3. **Types**: All extracted values are returned as strings. If you need to perform calculations, use the `transform`
   section to convert them (e.g., `float(value)`).

For complex structured data like JSON, using the [`jsonpath`](../jsonpath/README.md) parser is recommended over regex
for better reliability.
