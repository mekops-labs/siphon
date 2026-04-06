# Home Assistant (HASS) Collector

The `hass` collector polls the Home Assistant Core API for the state of specific entities at set intervals and publishes
the raw JSON response to the event bus.

## Configuration

The collector takes a polling interval. In the `topics` section, you map internal aliases (used by pipelines) to Home
Assistant `entity_id`s.

```yaml
collectors:
  home_assistant:
    type: "hass"
    params:
      interval: 60 # Poll every 60 seconds (default is 30)
    topics:
      living_room_temp: "sensor.living_room_temperature"
      main_door: "binary_sensor.front_door_contact"
```

## Quick How-To

1. **Register the Collector**: Add the `hass` type to your `collectors` section.
2. **Define Entities**: Map your desired aliases to HASS `entity_id`s in the `topics` block.
3. **Use in Pipeline**: In your pipeline, set `source_topic` to the alias you defined (e.g., `living_room_temp`).
4. **Data Format**: The collector sends the raw JSON object returned by the Home Assistant `/api/states/<entity_id>`
   endpoint.

## Requirements

- **Home Assistant Supervisor**: This collector is designed to run as a Home Assistant Add-on.
- **Authentication**: It automatically uses the `SUPERVISOR_TOKEN` environment variable provided by the Supervisor.
  Ensure the add-on has appropriate permissions to access the Home Assistant API.
