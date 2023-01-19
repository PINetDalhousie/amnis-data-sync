#!/bin/bash

# Bash script to generate key files, CARoot, and self-signed cert for use with SSL:
#Taken from https://stackoverflow.com/questions/51959495/how-to-create-kafka-python-producer-with-ssl-configuration

NAME=$1
PASSWORD="password"


# Offical site
sudo keytool -keystore server.keystore.jks -alias server -validity 365 -genkey -keyalg RSA -storetype pkcs12
echo 01 > serial.txt
sudo touch index.txt
sudo openssl req -x509 -config openssl-ca.cnf -newkey rsa:4096 -sha256 -nodes -out cacert.pem -outform PEM
sudo keytool -keystore client.truststore.jks -alias CARoot -import -file cacert.pem
sudo keytool -keystore server.truststore.jks -alias CARoot -import -file cacert.pem
sudo keytool -keystore server.keystore.jks -alias server -certreq -file cert-file
sudo openssl ca -config openssl-ca.cnf -policy signing_policy -extensions signing_req -out cert-signed -infiles cert-file
sudo keytool -keystore server.keystore.jks -alias CARoot -import -file cacert.pem
sudo keytool -keystore server.keystore.jks -alias server -import -file cert-signed
sudo keytool -exportcert -alias CARoot -keystore server.keystore.jks -rfc -file CARoot.pem

