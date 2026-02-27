#!/usr/bin/env bash
set -euo pipefail

# KDC setup for TEST.QAIL.IO realm.
# Creates principals and exports keytabs to /shared for postgres and client.

REALM="TEST.QAIL.IO"
KDC_HOST="kdc.test.qail.io"
PG_HOST="postgres.test.qail.io"
CLIENT_PRINCIPAL="qail_gss_user@${REALM}"
SERVICE_PRINCIPAL="postgres/${PG_HOST}@${REALM}"

echo "==> Installing MIT Kerberos KDC..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq && apt-get install -y -qq krb5-kdc krb5-admin-server >/dev/null 2>&1

echo "==> Configuring realm ${REALM}..."
cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    forwardable = true

[realms]
    ${REALM} = {
        kdc = ${KDC_HOST}
        admin_server = ${KDC_HOST}
    }

[domain_realm]
    .test.qail.io = ${REALM}
    test.qail.io = ${REALM}
EOF

# Copy krb5.conf to shared volume for other containers.
cp /etc/krb5.conf /shared/krb5.conf

echo "==> Creating KDC database..."
kdb5_util create -s -r "${REALM}" -P "kdc_master_password" 2>/dev/null

echo "==> Creating principals..."
kadmin.local -q "addprinc -pw client_password ${CLIENT_PRINCIPAL}" 2>/dev/null
kadmin.local -q "addprinc -randkey ${SERVICE_PRINCIPAL}" 2>/dev/null

echo "==> Exporting keytabs..."
kadmin.local -q "ktadd -k /shared/postgres.keytab ${SERVICE_PRINCIPAL}" 2>/dev/null
kadmin.local -q "ktadd -k /shared/client.keytab ${CLIENT_PRINCIPAL}" 2>/dev/null
chmod 600 /shared/*.keytab

echo "==> Starting KDC..."
krb5kdc -n &
KDC_PID=$!

echo "==> KDC ready (PID ${KDC_PID}), realm ${REALM}"
echo "    Service: ${SERVICE_PRINCIPAL}"
echo "    Client:  ${CLIENT_PRINCIPAL}"

# Keep running until container stops.
wait ${KDC_PID}
