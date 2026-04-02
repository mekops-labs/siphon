# Windy Sink

Sends weather data to [Windy.com](https://stations.windy.com/).

## Configuration

```yaml
sinks:
  windy_pws:
    type: "windy"
    params:
      apikey: "your-windy-api-key"
      id: 0 # Station ID
```

## Data Format

This sink expects a JSON payload from the pipeline. It automatically injects the `station` field into the JSON before
sending it to Windy. Example input: `{"temp": 21.5, "wind": 10}` becomes `{"temp": 21.5, "wind": 10, "station": 0}`.

API is defined in this [API reference](https://stations.windy.com/api-reference).
