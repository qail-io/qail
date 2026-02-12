# Soak Test Infrastructure

Podman-based monitoring stack for the Qail Gateway soak test on `gateway.qail.io`.

## Architecture

```
Internet → Traefik (443) → Qail Gateway (8080)
                         → Grafana (3000) via gateway.qail.io:3000
         → Prometheus (9090) scrapes Gateway /metrics
```

## Quick Start

```bash
# On the server
cd /opt/qail-soak
podman-compose up -d

# Check status
podman-compose ps
```

## Access

- **Gateway:** https://gateway.qail.io
- **Grafana:** http://gateway.qail.io:3000 (admin/qailsoak2026)
- **Prometheus:** http://gateway.qail.io:9090
