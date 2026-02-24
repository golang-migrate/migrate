#!/usr/bin/env bash
set -euo pipefail

# --- parameters (tune if needed) ---
DAYS_CA=3650        # ~10 years
DAYS_CERT=3650      # ~10 years
RSA_BITS_CA=2048
RSA_BITS_CERT=2048

# Outputs (requested + CA private key saved)
CA_CERT_PEM="ca.pem"
CA_KEY_PEM="ca-key.pem"
CERT_PEM="cert.pem"
KEY_PEM="key.pem"

# Make Issuer exactly CN=localhost
CA_CN="localhost"
CERT_CN="localhost"

# --- work dir for temp files ---
TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl not found"; exit 1; }

umask 077  # ensure new keys are not world-readable

# 1) Create & save CA private key
openssl genrsa -out "$CA_KEY_PEM" "$RSA_BITS_CA"
chmod 600 "$CA_KEY_PEM"

# 2) Create self-signed CA cert (Issuer=CN=localhost, Subject=CN=localhost)
openssl req -x509 -new -nodes \
  -key "$CA_KEY_PEM" \
  -sha256 -days "$DAYS_CA" \
  -subj "/CN=${CA_CN}" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  -out "$CA_CERT_PEM"

# 3) Create server key
openssl genrsa -out "$KEY_PEM" "$RSA_BITS_CERT"
chmod 600 "$KEY_PEM"

# 4) CSR for localhost with SAN (modern clients require SAN)
openssl req -new \
  -key "$KEY_PEM" \
  -subj "/CN=${CERT_CN}" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -out "$TMPDIR/server.csr"

# 5) Sign server cert with CA
cat > "$TMPDIR/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req \
  -in "$TMPDIR/server.csr" \
  -CA "$CA_CERT_PEM" -CAkey "$CA_KEY_PEM" \
  -CAcreateserial -CAserial "$TMPDIR/ca.srl" \
  -out "$CERT_PEM" \
  -days "$DAYS_CERT" -sha256 \
  -extfile "$TMPDIR/server.ext"

# 6) Quick checks
echo "Generated:"
echo "  - $CA_CERT_PEM   (CA certificate; Issuer/Subject CN=localhost)"
echo "  - $CA_KEY_PEM    (CA private key; keep secure)"
echo "  - $CERT_PEM      (localhost certificate signed by CA)"
echo "  - $KEY_PEM       (localhost private key)"
echo
echo "Verify chain:"
openssl verify -CAfile "$CA_CERT_PEM" "$CERT_PEM" || true
echo
echo "Show issuer/subject:"
openssl x509 -in "$CA_CERT_PEM" -noout -subject -issuer
openssl x509 -in "$CERT_PEM" -noout -subject -issuer -ext subjectAltName

