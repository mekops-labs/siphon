# Home Assistant (HASS) Collector

The `hass` collector polls the Home Assistant Core API for the state of specific entities at set intervals and publishes
the raw JSON response to the event bus.

## Configuration

The collector takes a polling interval. In the `topics` section, you map internal aliases (used by pipelines) to Home
Assistant `entity_id`s.

```yaml
collectors:
  # 1. Define the Home Assistant Collector
  local_ha:
    type: hass
    params:
      interval: 60  # Poll the Home Assistant API every 60 seconds
    topics:
      # Map an Alias -> HA Entity ID
      outdoor_temp: "sensor.backyard_temperature"
      living_room: "climate.living_room"

pipelines:
  # 2. Process the data from Home Assistant
  - name: process_ha_temps
    topics: ["outdoor_temp"] # Listen to the alias!
    bus_mode: volatile
    parser:
      type: jsonpath
      vars:
        # The HA API always returns a standard JSON object.
        # You can extract the main state or nested attributes.
        temp: "$.state"
        unit: "$.attributes.unit_of_measurement"

    # 3. Transform and Dispatch...
    transform:
      - tempStr: "string(float(temp))+' '+unit"
```

## Quick How-To

1. **Register the Collector**: Add the `hass` type to your `collectors` section.
2. **Define Entities**: Map your desired aliases to HASS `entity_id`s in the `topics` block.
3. **Use in Pipeline**: In your pipeline, set `source_topic` to the alias you defined (e.g., `living_room_temp`).
4. **Data Format**: The collector sends the raw JSON object returned by the Home Assistant `/api/states/<entity_id>`
   endpoint.
5. **Wildcard**: If '*' is used as topic, then `/api/states` is called and json with all entities is returned.

### Example for wildcard topics

```yaml
collectors:
  home_assistant:
    type: "hass"
    params:
      interval: 60 # Poll every 60 seconds (default is 30)
    topics:
      entities: "*"

pipelines:
  - name: process_ha_entities
    topics: ["entities"] # Listen to the alias!
    bus_mode: volatile
    parser:
      type: jsonpath
      vars:
        # The HA API returns all state in the form of object array, we need to filter it
        temp: $[?(@.entity_id == "sensor.gw3000a_outdoor_temperature")].state
        humidity: $[?(@.entity_id == "sensor.gw3000a_humidity")].state
    transform:
      # filter in jsonpath returns array, so we need to get first element and make sure it's number
      - temp: float(temp[0])
      - humidity: float(humidity[0])
```

## Requirements

- **Home Assistant Supervisor**: This collector is designed to run as a Home Assistant Add-on.
- **Authentication**: It automatically uses the `SUPERVISOR_TOKEN` environment variable provided by the Supervisor.
  Ensure the add-on has appropriate permissions to access the Home Assistant API.
