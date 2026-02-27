#!/usr/bin/env bash
set -euo pipefail

# Client-side test runner for Kerberos E2E.
# Obtains a Kerberos ticket, then runs the qail-pg GSS smoke test.

echo "==> Waiting for krb5.conf..."
for i in $(seq 1 30); do
    [ -f /shared/krb5.conf ] && break
    sleep 1
done

if [ ! -f /shared/krb5.conf ]; then
    echo "FATAL: /shared/krb5.conf not found after 30s"
    exit 1
fi

echo "==> Obtaining Kerberos ticket..."
# Use the client keytab (non-interactive, no password prompt).
kinit -kt /shared/client.keytab qail_gss_user@TEST.QAIL.IO

echo "==> Ticket cache:"
klist

echo "==> Running qail-pg GSS smoke test..."
cargo test -p qail-pg --features enterprise-gssapi \
    --test gss_linux_smoke -- --ignored --nocapture

echo "==> All tests passed!"
