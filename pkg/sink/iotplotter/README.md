# IoTPlotter Sink

Sends data to [IoTPlotter](https://iotplotter.com/).

## Configuration

```yaml
sinks:
  plotter:
    type: "iotplotter"
    params:
      apikey: "your-api-key"
      feed: "your-feed-id"
      url: "https://iotplotter.com" # Optional, default provided
```

Note: The payload sent to this sink should be in the format expected by IoTPlotter's [JSON
API](https://iotplotter.com/docs/#json-formatted).
