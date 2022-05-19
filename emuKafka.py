#!/usr/bin/python3

import multiprocessing
from mininet.net import Mininet
from mininet.util import pmonitor

import os
import sys
import subprocess
import time

def configureKafkaCluster(brokerPlace, zkPlace, args):
	print("Configure kafka cluster")

	propertyFile = open("kafka-3.1.0/config/kraft/server.properties", "r")
	serverProperties = propertyFile.read()
	
	# Specify controller addresses to connect
	controllerAddresses = ""
	controllerPort = 19092

	for i in range(len(brokerPlace)-1):
		controllerAddresses += str(i+1) + "@10.0.0." + str(i+1) + ":" +str(controllerPort)+","
		controllerPort += 1

	controllerAddresses += str(len(brokerPlace)) + "@10.0.0." + str(len(brokerPlace)) + ":" +str(controllerPort)

	controllerPort = 19092
	for bID in brokerPlace:
		os.system("sudo mkdir kafka-3.1.0/kafka" + str(bID) + "/")

		bProperties = serverProperties
		bProperties = bProperties.replace("node.id=1", "node.id="+str(bID))
		bProperties = bProperties.replace("controller.quorum.voters=1@localhost", 
		"controller.quorum.voters=" + controllerAddresses)
		bProperties = bProperties.replace(
			"listeners=PLAINTEXT://:9092,CONTROLLER://:9093", 
			"listeners=PLAINTEXT://:9092,CONTROLLER://10.0.0." + str(bID) + ":" + str(controllerPort))
		controllerPort += 1
		bProperties = bProperties.replace(
			"advertised.listeners=PLAINTEXT://localhost:9092", 
			"advertised.listeners=PLAINTEXT://10.0.0." + str(bID) + ":9092")
		bProperties = bProperties.replace("log.dirs=/tmp/kraft-combined-logs",
			"log.dirs=./kafka-3.1.0/logs/kafka" + str(bID))

		bProperties = bProperties.replace(
			"listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL", 
			"#listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL")
							
		bProperties = bProperties.replace("#replica.fetch.wait.max.ms=500", "replica.fetch.wait.max.ms="+str(args.replicaMaxWait))
		bProperties = bProperties.replace("#replica.fetch.min.bytes=1", "replica.fetch.min.bytes="+str(args.replicaMinBytes))

		bFile = open("kafka-3.1.0/config/kraft/server" + str(bID) + ".properties", "w")
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



# Function to log open processes running on host
def monitorCmd(popens, logDir):	
	for host, line in pmonitor(popens):
		if host:
			kraftLog = open(logDir +"/kraft/server-" + host.name + ".log", "a")
			kraftLog.write("<%s>: %s" % (host.name, line))


def runKafka(net, brokerPlace, logDir, brokerWaitTime=200):

	netNodes = {}
	# Run the following in terminal to get new uuid:
	# ./bin/kafka-storage.sh random-uuid
	uuid = 'JmL9ihFRQmSabrNWrYSpjg'
	
	# Make kraft log foldr
	os.system("sudo mkdir -p " + logDir + "/kraft/")

	for node in net.hosts:
		netNodes[node.name] = node
		
	startTime = time.time()
	for bNode in brokerPlace:					
		print("Setting Kafka storage for node "+str(bNode))
		os.system(f"kafka-3.1.0/bin/kafka-storage.sh format -t {uuid} -c kafka-3.1.0/config/kraft/server{str(bNode)}.properties")				
		time.sleep(1)		


	popens = {}
	for bNode in brokerPlace:
		bID = "h"+str(bNode)
		startingHost = netNodes[bID]		
		h = netNodes[bID]		
		print("Creating Kafka broker at node "+str(bNode))	
		popens[h] = h.popen("kafka-3.1.0/bin/kafka-server-start.sh kafka-3.1.0/config/kraft/server"+str(bNode)+".properties &", shell=True)									
		time.sleep(1)

	# Start the process logging monitor for mininet hosts		
	process = multiprocessing.Process(target=monitorCmd, args=(popens, logDir))
	process.start()
	time.sleep(10)

	brokerWait = True
	totalTime = 0	
	for bNode in brokerPlace:		
		while brokerWait:
			print("Testing Connection to Broker " + str(bNode) + "...")
			out, err, exitCode = startingHost.pexec("nc -z -v 10.0.0." + str(bNode) + " 9092")			
			stopTime = time.time()
			totalTime = stopTime - startTime
			if(exitCode == 0):
				brokerWait = False
	        #elif(totalTime > brokerWaitTime):
	        #    print("ERROR: Timed out waiting for Kafka brokers to start")
	        #    sys.exit(1)
			else:
				print("Waiting for Broker " + str(bNode) + " to Start...")
				time.sleep(10)
		brokerWait = True
	print("Successfully Created Kafka Brokers in " + str(totalTime) + " seconds")


def cleanKafkaState(brokerPlace):
	for bID in brokerPlace:
		os.system("sudo rm -rf kafka-3.1.0/kafka" + str(bID) + "/")
		os.system("sudo rm -f kafka-3.1.0/config/kraft/server" + str(bID) + ".properties")









