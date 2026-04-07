# REST API Collector

The REST collector acts like a scheduled `curl` command. It periodically polls external HTTP endpoints (APIs, smart devices, web servers) and publishes the response payloads (like JSON or plain text) to the Siphon Event Bus for parsing.

## Configuration

```yaml
collectors:
  weather_api:
    type: rest
    params:
      interval: 600           # Poll every 10 minutes
      method: "GET"           # Optional: Defaults to GET
      timeout: 15             # Optional: Request timeout in seconds
      headers:
        "Authorization": "Bearer %%MY_API_KEY%%"
        "Accept": "application/json"
    topics:
      # ALIAS -> TARGET URL
      local_weather: "https://api.weather.com/v3/wx/conditions/current?stationId=KNYC"
```
