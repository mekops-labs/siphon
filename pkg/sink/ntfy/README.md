# Ntfy Sink

Sends notifications to ntfy.

## Configuration

```yaml
sinks:
  push_alerts:
    type: "ntfy"
    params:
      url: "https://ntfy.sh"
      topic: "my_alerts"
      token: "tk_..."     # Optional: Access token
      title: "Status"     # Optional: Message title
      priority: 3         # Optional: 1-5
```

The raw payload from the pipeline will be used as the message body.
