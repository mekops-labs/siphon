# MQTT Sink

The MQTT sink publishes the final pipeline payload to a specified MQTT broker. This is highly useful for sending processed data, aggregations, or alerts back to Home Assistant or other IoT dashboards.

## Configuration

```yaml
sinks:
  my_broker:
    type: mqtt
    params:
      url: "tcp://192.168.1.100:1883"
      user: "%%MQTT_USER%%"    # Supports Siphon's environment variable injection
      pass: "%%MQTT_PASS%%"
      topic: "siphon/processed_data"
      qos: 1                   # Optional: Quality of Service (0, 1, or 2). Defaults to 0.
      retained: true           # Optional: Retain message on broker. Defaults to false.
```
