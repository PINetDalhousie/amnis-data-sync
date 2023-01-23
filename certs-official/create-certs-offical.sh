#!/bin/bash

# Bash script to generate key files, CARoot, and self-signed cert for use with SSL:

NAME=$1
PASSWORD="password"

#-storepass
# Official site
sudo keytool -keystore server.keystore.jks -alias server -validity 365 -genkey -keyalg RSA -storetype pkcs12 -storepass $PASSWORD
#echo 01 > serial.txt
#sudo touch index.txt
sudo openssl req -x509 -config openssl-ca.cnf -newkey rsa:4096 -sha256 -nodes -out cacert.pem -outform PEM
sudo keytool -keystore client.truststore.jks -alias CARoot -import -file cacert.pem -storepass $PASSWORD
sudo keytool -keystore server.truststore.jks -alias CARoot -import -file cacert.pem -storepass $PASSWORD
sudo keytool -keystore server.keystore.jks -alias server -certreq -file cert-file -storepass $PASSWORD
sudo openssl ca -config openssl-ca.cnf -policy signing_policy -extensions signing_req -out cert-signed -infiles cert-file
sudo keytool -keystore server.keystore.jks -alias CARoot -import -file cacert.pem -storepass $PASSWORD
sudo keytool -keystore server.keystore.jks -alias server -import -file cert-signed -storepass $PASSWORD
sudo keytool -exportcert -alias CARoot -keystore server.keystore.jks -rfc -file CARoot.pem

