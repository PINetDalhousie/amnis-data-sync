#!/usr/bin/python3

from mininet.net import Mininet

import os
import sys
import subprocess
import time

def configureKafkaCluster(brokerPlace, zkPlace):
	print("Configure kafka cluster")

	propertyFile = open("kafka/config/server.properties", "r")
	serverProperties = propertyFile.read()

	for bID in brokerPlace:
		os.system("sudo mkdir kafka/kafka" + str(bID) + "/")

		bProperties = serverProperties
		bProperties = bProperties.replace("broker.id=0", "broker.id="+str(bID))
		bProperties = bProperties.replace(
			"#advertised.listeners=PLAINTEXT://your.host.name:9092", 
			"advertised.listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092")
		bProperties = bProperties.replace("log.dirs=/tmp/kafka-logs",
			"log.dirs=./kafka/kafka" + str(bID))

		#Specify zookeeper addresses to connect
		zkAddresses = ""
		zkPort = 2181

		for i in range(len(zkPlace)-1):
			zkAddresses += "localhost:"+str(zkPort)+","
			zkPort += 1

		zkAddresses += "localhost:"+str(zkPort)

		bProperties = bProperties.replace(
			"zookeeper.connect=localhost:2181",
			"zookeeper.connect="+zkAddresses)

		bFile = open("kafka/config/server" + str(bID) + ".properties", "w")
		bFile.write(bProperties)
		bFile.close()

	propertyFile.close()



def placeKafkaBrokers(net, nBroker, nZk):

	brokerPlace = []
	zkPlace = []

	if nBroker < 0 or nBroker > len(net.hosts):
		print("ERROR: Cannot support specified number of broker instances.");
		sys.exit(1)
	elif nZk < 0 or nZk > len(net.hosts):
		print("ERROR: Cannot support specified number of Zookeeper instances.");
		sys.exit(1)	

	if nBroker == len(net.hosts):
		for i in range(nBroker):
			brokerPlace.append(i+1)
	else:
		print("ERROR: Support for broker placement will be added in the future. Please consider setting the number of brokers to the number of end hosts in your network.")
		sys.exit(1)

	if nZk == len(net.hosts):
		for i in range(nZk):
			zkPlace.append(i+1)
	else:
		print("ERROR: Support for zookeeper placement will be added in the future. Please consider setting the number of zookeeper instances to the number of end hosts in your network.")
		sys.exit(1)

	return brokerPlace, zkPlace



def runKafka(net, brokerPlace):

	netNodes = {}

	for node in net.hosts:
		netNodes[node.name] = node

	for bNode in brokerPlace:
		bID = "h"+str(bNode)

		startingHost = netNodes[bID]

		startingHost.popen("kafka/bin/kafka-server-start.sh kafka/config/server"+str(bNode)+".properties &", shell=True)
		time.sleep(10)
		print("Created Kafka broker at node "+str(bNode))


def cleanKafkaState(brokerPlace):
	for bID in brokerPlace:
		os.system("sudo rm -r kafka/kafka" + str(bID) + "/")
		os.system("sudo rm kafka/config/server" + str(bID) + ".properties")









