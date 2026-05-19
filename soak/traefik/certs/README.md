# Local TLS Fixtures

Do not commit private keys or origin certificates in this directory.

For local soak testing, place these files here outside Git tracking:

- `cert.pem`
- `key.pem`

Example self-signed fixture:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout soak/traefik/certs/key.pem \
  -out soak/traefik/certs/cert.pem \
  -days 30 \
  -subj '/CN=gateway.example.com'
```
