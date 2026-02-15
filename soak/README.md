# Soak Test Infrastructure

Podman-based monitoring stack for the Qail Gateway soak test on `gateway.example.com`.

## Architecture

```
Internet → Traefik (443) → Qail Gateway (8080)
                         → Grafana (3000) via gateway.example.com:3000
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

- **Gateway:** https://gateway.example.com
- **Grafana:** http://gateway.example.com:3000 (admin/changeme)
- **Prometheus:** http://gateway.example.com:9090
