#!/usr/bin/python3

import sys
import os

def configureKafkaMysqlConnection(brokerPlace):
	print("Configure kafka MySQL connection")

	propertyFile = open("kafka/config/connect-standalone.properties", "r")
	serverProperties = propertyFile.read()

	bProperties = serverProperties

	brokerAddresses = ""
	brokerPort = 9092
	for i in range(len(brokerPlace)-1):
		brokerAddresses += "10.0.0." + str(brokerPlace[i]) + ":" +str(brokerPort)+","
	brokerAddresses += "10.0.0."+str(brokerPlace[-1])+ ":" +str(brokerPort)

	bProperties = bProperties.replace("bootstrap.servers=localhost:9092", \
		"bootstrap.servers="+brokerAddresses)
	bProperties = bProperties.replace(
		"key.converter.schemas.enable=true", 
		"key.converter.schemas.enable=false")
	bProperties = bProperties.replace("#plugin.path=",
		"plugin.path=dependency/mysql-connector")

	bFile = open("kafka/config/connect-standalone-new.properties", "w")
	bFile.write(bProperties)
	bFile.close()

	propertyFile.close()

def cleanMysqlState():
	os.system("sudo rm -rf kafka/config/connect-standalone-new.properties")