# Shell Collector

The `shell` collector periodically executes shell commands and captures their `stdout` to be processed by pipelines.

## Configuration

```yaml
collectors:
  cli_tools:
    type: "shell"
    params:
      interval: 60 # Run every minute
    topics:
      - free_space: "df -h / --output=pcent | tail -1"
```

## Quick How-To

1. **Command Execution**: Commands are wrapped in `sh -c`, so you can use pipes (`|`), redirects, and other shell
   features.
2. **Output**: Only the standard output (`stdout`) is captured. If the command fails (returns non-zero), the error is
   logged and no data is published.
3. **Topics**: The `topics` in the pipeline configuration are exactly the commands string to be executed.
