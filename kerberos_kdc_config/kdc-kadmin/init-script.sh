#!/bin/bash

set -e

KADMIN_PRINCIPAL_FULL=$KADMIN_PRINCIPAL@$REALM
KDC_HOSTNAME=$(hostname -f)

# krb5.conf
cat <<EOF > /etc/krb5.conf
[libdefaults]
  default_realm = $REALM

[realms]
  $REALM = {
    kdc = $KDC_HOSTNAME
    admin_server = $KDC_HOSTNAME
  }
EOF

# kdc.conf
cat <<EOF > /etc/krb5kdc/kdc.conf
[realms]
  $REALM = {
    acl_file = /etc/krb5kdc/kadm5.acl
    supported_enctypes = $SUPPORTED_ENCRYPTION_TYPES
    default_principal_flags = +preauth
  }
EOF

# kadm5.acl
cat <<EOF > /etc/krb5kdc/kadm5.acl
$KADMIN_PRINCIPAL_FULL *
noPermissions@$REALM X
EOF

# Realm 초기화
echo "Creating new Kerberos realm..."
MASTER_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 30)
krb5_newrealm <<EOF
$MASTER_PASSWORD
$MASTER_PASSWORD
EOF

# 관리자 Principal 등록
echo "Creating admin principal: $KADMIN_PRINCIPAL_FULL"
kadmin.local -q "delete_principal -force $KADMIN_PRINCIPAL_FULL" || true
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD $KADMIN_PRINCIPAL_FULL"

# 테스트용 Principal
echo "Creating noPermissions principal"
kadmin.local -q "delete_principal -force noPermissions@$REALM" || true
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD noPermissions@$REALM"

# 서비스 시작
krb5kdc
kadmind -nofork
