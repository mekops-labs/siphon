# MQTT Collector

The `mqtt` collector subscribes to topics on an MQTT broker and bridges the incoming messages into the Siphon event bus.

## Configuration

```yaml
collectors:
  mosquitto:
    type: "mqtt"
    params:
      url: "tcp://localhost:1883"
      user: "siphon_user"
      pass: "mqtt_pass"
    topics:
      - livingroom: "home/livingroom/sensors/#"
```

## Quick How-To

1. **Broker URL**: Use the standard `tcp://` or `ssl://` schemes.
2. **Authentication**: Optional `user` and `pass` can be provided.
3. **Wildcards**: You can use standard MQTT wildcards (`+` and `#`) in the `source_topic` of your pipelines.
4. **Automatic Reconnect**: The collector is configured to automatically attempt reconnection if the broker goes offline.
5. **Topic Preservation**: The actual MQTT topic is preserved as the event topic, allowing for flexible routing.
