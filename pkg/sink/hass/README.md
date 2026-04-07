# Home Assistant (HASS) Sink

The HASS sink seamlessly integrates with Home Assistant's MQTT Auto-Discovery feature.

When Siphon starts, this sink automatically creates a device in Home Assistant named **"Siphon ETL Engine"** and registers your configured entity (e.g., a sensor or switch) under it. Any data dispatched to this sink will instantly update the state of that entity in Home Assistant.

## Configuration

```yaml
sinks:
  custom_ha_sensor:
    type: hass
    params:
      # MQTT Connection Details (Automatically injected if using the HA Add-on!)
      url: "tcp://%%MQTT_HOST%%:%%MQTT_PORT%%"
      user: "%%MQTT_USER%%"
      pass: "%%MQTT_PASS%%"

      # Auto-Discovery Payload
      object_id: "aggregated_power"    # Creates sensor.aggregated_power in HA
      name: "Total House Power Draw"   # Friendly Name
      component: "sensor"              # HA Component (sensor, binary_sensor, etc.)
      device_class: "power"
      state_class: "measurement"
      unit_of_measurement: "W"
      icon: "mdi:flash"
```

## Availability & Reliability

The HASS sink automatically manages the entity's availability status using MQTT **Last Will and Testament (LWT)**.

- **Birth Message:** When Siphon connects to the MQTT broker, it immediately publishes `online` to the availability topic and sends the discovery configuration.
- **Last Will:** If Siphon disconnects unexpectedly, the MQTT broker automatically publishes `offline` to the availability topic. This ensures that Home Assistant correctly marks the entity as "Unavailable" instead of displaying stale data.

By default, the availability topic is derived from the `object_id`, but it can be overridden using the `availability_topic` parameter.

## Usage in Pipelines

To use the HASS sink, reference it by the name defined in your `sinks` section within a pipeline. Siphon will dispatch the resulting state of the pipeline to the HASS entity.

```yaml
pipelines:
  - name: "home-power-aggregator"
    topics:
      - "telemetry/main_meter"
    parser:
      type: "jsonpath"
      vars:
        power: "$.watts"
    transform:
      power_kw: "power / 1000"
    sinks:
      - name: "custom_ha_sensor"
```
