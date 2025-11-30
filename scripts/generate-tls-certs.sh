#!/bin/bash

# Generate TLS certificates for Kafka
# This script creates a Certificate Authority (CA) and broker/client certificates

set -e

CERTS_DIR="./tls-certs"
VALIDITY_DAYS=365
PASSWORD="kafkatest123"

echo "🔐 Generating TLS certificates for Kafka..."

# Create directory structure
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

# 1. Generate CA (Certificate Authority)
echo "📝 Step 1/5: Creating Certificate Authority..."
openssl req -new -x509 -keyout ca-key -out ca-cert -days $VALIDITY_DAYS \
  -passin pass:$PASSWORD -passout pass:$PASSWORD \
  -subj "/C=US/ST=CA/L=SanFrancisco/O=GatlingKafka/OU=Testing/CN=GatlingCA"

# 2. Create broker keystore
echo "📝 Step 2/5: Creating broker keystore..."
keytool -keystore kafka.broker.keystore.jks -alias broker -validity $VALIDITY_DAYS \
  -genkey -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD \
  -dname "CN=localhost,OU=Testing,O=GatlingKafka,L=SanFrancisco,ST=CA,C=US" \
  -ext SAN=DNS:localhost,DNS:kafka,IP:127.0.0.1

# 3. Create broker certificate signing request (CSR)
echo "📝 Step 3/5: Creating broker CSR..."
keytool -keystore kafka.broker.keystore.jks -alias broker -certreq -file broker-cert-sign-request \
  -storepass $PASSWORD -keypass $PASSWORD

# 4. Sign the broker certificate with CA
echo "📝 Step 4/5: Signing broker certificate..."
openssl x509 -req -CA ca-cert -CAkey ca-key -in broker-cert-sign-request \
  -out broker-cert-signed -days $VALIDITY_DAYS -CAcreateserial \
  -passin pass:$PASSWORD \
  -extensions SAN -extfile <(printf "[SAN]\nsubjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1")

# 5. Import CA and signed certificate into broker keystore
echo "📝 Step 5/5: Importing certificates into keystore..."
keytool -keystore kafka.broker.keystore.jks -alias CARoot -import -file ca-cert \
  -storepass $PASSWORD -keypass $PASSWORD -noprompt

keytool -keystore kafka.broker.keystore.jks -alias broker -import -file broker-cert-signed \
  -storepass $PASSWORD -keypass $PASSWORD -noprompt

# 6. Create truststore
echo "📝 Creating truststore..."
keytool -keystore kafka.broker.truststore.jks -alias CARoot -import -file ca-cert \
  -storepass $PASSWORD -keypass $PASSWORD -noprompt

# 7. Create client truststore (for Gatling)
echo "📝 Creating client truststore..."
keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert \
  -storepass $PASSWORD -keypass $PASSWORD -noprompt

# 8. Export CA certificate in PEM format for clients
openssl x509 -in ca-cert -out ca-cert.pem -outform PEM

echo "✅ TLS certificates generated successfully!"
echo ""
echo "📁 Files created in $CERTS_DIR/:"
ls -lh
echo ""
echo "🔑 Keystore/Truststore password: $PASSWORD"
echo ""
echo "📋 Next steps:"
echo "  1. Run: docker-compose up -d"
echo "  2. Update your Kafka client configuration to use TLS"
echo "  3. Use kafka.client.truststore.jks in your Gatling tests"
