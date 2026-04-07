# Webhook Collector

The Webhook collector provides a high-performance, secure HTTP listener that allows external systems (GitHub, Stripe,
Grafana, custom scripts) to push data directly into Siphon.

The current implementation is **hardened for public internet exposure**, featuring multi-layer defense against DDoS
attacks, memory exhaustion, and unauthorized access.

## Configuration

```yaml
collectors:
  secure_api:
    type: webhook
    params:
      port: 8080
      # 1. Authentication (Optional but Recommended)
      token: "%%MY_WEBHOOK_SECRET%%"

      # 2. DDoS Protection (Rate Limiting)
      rps: 5.0     # Sustained Requests Per Second allowed
      burst: 10    # Maximum concurrent burst allowed

      # 3. Memory Protection
      max_body_mb: 2  # Hard limit on payload size (Megabytes)

      # 4. Internal Idempotency (Duplicate Detection)
      dedupe_ttl: 300 # Ignore identical payloads for 300s (5 mins)

    topics:
      # ALIAS -> URL PATH
      github_hooks: "/webhooks/github"
      sensor_push: "/data/sensor01"
```

## Security Features

### 🛡️ Layered Defense

1. **Bearer Token Auth:** If a `token` is defined, the collector rejects any request that doesn't include the matching
   `Authorization: Bearer <token>` header.
2. **Token Bucket Rate Limiting:** Prevents brute-force and DDoS attacks by strictly controlling the flow of incoming
   requests.
3. **Automatic Deduplication:** Siphon calculates a SHA-256 hash of every incoming payload. If an identical hash is
   received within the `dedupe_ttl` window, Siphon returns `200 OK` to the sender but silently drops the event to
   prevent duplicate pipeline processing.
4. **Payload Constraints:** Uses `http.MaxBytesReader` to terminate connections immediately if a client attempts to send
   a massive file to crash the system's memory.
5. **Slowloris Protection:** Implements strict `ReadTimeout` and `IdleTimeout` at the socket level to prevent attackers
   from holding connections open indefinitely.

## Usage Example

To push data to a hardened Siphon endpoint via `curl`:

```bash
curl -X POST http://siphon-ip:8080/data/sensor01 \
  -H "Authorization: Bearer YOUR_SECRET_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"temperature": 22.5, "unit": "C"}'
```

## Best Practices

* **Use HTTPS:** While Siphon is hardened, it does not handle TLS natively. Always place Siphon behind a Reverse Proxy
  (like Nginx, Caddy, or Traefik) to provide encryption.
* **Environment Variables:** Ensure to store your `token` in the Home Assistant Add-on "Env Vars" or as a system
  environment variable, and reference it using `%%TOKEN_NAME%%`.
