#!/bin/bash

# Bash script to generate key files, CARoot, and self-signed cert for use with SSL:
# Source: https://stackoverflow.com/questions/51959495/how-to-create-kafka-python-producer-with-ssl-configuration

NAME=$1
PASSWORD="password"

echo "Creating certificate for server"

# Step 1 - Create SSL key for broker. The keystore.jks file stores the machines own identity.
# Ideally this step is run on the Kafka broker that the key will be used on, as this key should never be transmitted/leave the server that it is intended for.
keytool -keystore server.keystore.jks -alias server -validity 365 -genkey -keyalg RSA 

# Step 2 - Create a certificate authority
# The CA is simply a public/private key pair (cs-key) and certificate (ca-cert) that is signed by itself, and is only intended to sign other certificates.
# Only run once?
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365

# Add the generated CA to the brokers' truststore so that the brokers can trust this CA
# The truststore stores all the certificates that the machine should trust.
# Importing a cert into one's truststore also means trusting all certs that are signed by that cert.
keytool -keystore server.truststore.jks -alias CARoot -import -file ca-cert
# Add the generated CA to the clients' truststore so that the clients can trust this CA
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert

# Step 3 - Sign the cert
keytool -keystore server.keystore.jks -alias server -certreq -file cert-file
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:$PASSWORD
keytool -keystore server.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore server.keystore.jks -alias server -import -file cert-signed

# You can then use the following command to extract the CARoot.pem:
keytool -exportcert -alias CARoot -keystore server.keystore.jks -rfc -file CARoot.pem

# TODO: Disable hostname verification for development
#ssl.endpoint.identification.algorithm=