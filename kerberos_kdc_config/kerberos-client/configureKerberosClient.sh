#!/bin/bash

set -e

KADMIN_PRINCIPAL_FULL=$KADMIN_PRINCIPAL@$REALM

cat <<EOF > /etc/krb5.conf
[libdefaults]
  default_realm = $REALM

[realms]
  $REALM = {
    kdc = kdc-kadmin
    admin_server = kdc-kadmin
  }
EOF

echo "Waiting for KDC to become available..."
until kadmin -p $KADMIN_PRINCIPAL_FULL -w $KADMIN_PASSWORD -q "listprincs"; do
  echo "KDC not ready yet. Retrying..."
  sleep 2
done

echo "Kerberos client connected to KDC successfully!"
