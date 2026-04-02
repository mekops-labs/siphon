# File Collector

The `file` collector reads the contents of local files at specified intervals and publishes the raw data to the event bus.

## Configuration

The collector itself takes a polling interval, and each pipeline defines the file path it wants to monitor.

```yaml
collectors:
  local_logs:
    type: "file"
    params:
      interval: 30 # Poll every 30 seconds

pipelines:
  - name: "system-monitor"
    source_topic: "/var/log/syslog" # The "topic" for a file collector is the file path
    # ... rest of pipeline
```

## Quick How-To

1. **Register the Collector**: Add the `file` type to your `collectors` section.
2. **Set the Path**: In your pipeline, set `source_topic` to the absolute or relative path of the file you want to ingest.
3. **Formatting**: Note that the entire file content is sent as a single payload. This is ideal for status files or small logs.
