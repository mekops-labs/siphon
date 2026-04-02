# Gotify Sink

Sends notifications to a Gotify server.

## Configuration

Register this sink in your configuration file:

```yaml
sinks:
  my_alerts:
    type: "gotify"
    params:
      url: "https://gotify.example.com"
      token: "A6789BCDE"
      title: "Siphon Alert" # Optional, default: "Siphon Alert"
      priority: 5          # Optional
```
